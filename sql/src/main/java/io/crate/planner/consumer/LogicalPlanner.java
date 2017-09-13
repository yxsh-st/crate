/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.consumer;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    interface NewQueriedRelation extends QueriedRelation {

        List<Symbol> outputs();

        WhereClause where();

        Optional<List<Symbol>> groupKeys();

        Optional<HavingClause> having();

        Optional<OrderBy> orderBy();

        Optional<Symbol> limit();

        Optional<Symbol> offset();



        Optional<List<Function>> aggregates();

        List<Symbol> aggregationSources();
    }

    static class WrappedRelation implements NewQueriedRelation {

        private final QueriedRelation relation;
        private final Optional<List<Function>> aggregates;
        private final List<Symbol> aggregationSources;

        public WrappedRelation(QueriedRelation relation) {
            this.relation = relation;
            SplitPoints splitPoints = SplitPoints.create(relation.querySpec());
            if (splitPoints.aggregates().isEmpty()) {
                this.aggregates = Optional.empty();
            } else {
                List<io.crate.analyze.symbol.Function> aggregates = splitPoints.aggregates();
                this.aggregates = Optional.of(aggregates);
            }
            this.aggregationSources = splitPoints.toCollect();
        }

        @Override
        public List<Symbol> outputs() {
            return relation.querySpec().outputs();
        }

        @Override
        public WhereClause where() {
            return relation.querySpec().where();
        }

        @Override
        public Optional<List<Symbol>> groupKeys() {
            return relation.querySpec().groupBy();
        }

        @Override
        public Optional<HavingClause> having() {
            return relation.querySpec().having();
        }

        @Override
        public Optional<OrderBy> orderBy() {
            return relation.querySpec().orderBy();
        }

        @Override
        public Optional<Symbol> limit() {
            return relation.querySpec().limit();
        }

        @Override
        public Optional<Symbol> offset() {
            return relation.querySpec().offset();
        }

        @Override
        public Optional<List<Function>> aggregates() {
            return aggregates;
        }

        @Override
        public List<Symbol> aggregationSources() {
            return aggregationSources;
        }

        @Override
        public QuerySpec querySpec() {
            return relation.querySpec();
        }

        @Override
        public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
            return relation.accept(visitor, context);
        }

        @Override
        public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
            return relation.getField(path, operation);
        }

        @Override
        public List<Field> fields() {
            return relation.fields();
        }

        @Override
        public QualifiedName getQualifiedName() {
            return relation.getQualifiedName();
        }

        @Override
        public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
            relation.setQualifiedName(qualifiedName);
        }
    }

    public interface LogicalPlan {

        Plan makePhysical(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order);

        LogicalPlan tryCollapse();

        List<Symbol> outputs();
    }


    public Plan plan(QueriedRelation queriedRelation,
                     Planner.Context plannerContext,
                     ProjectionBuilder projectionBuilder) {
        LogicalPlanner.LogicalPlan logicalPlan = plan(queriedRelation).tryCollapse();

        return logicalPlan.makePhysical(
            plannerContext,
            projectionBuilder,
            LogicalPlanner.NO_LIMIT,
            0,
            null
        );
    }

    private LogicalPlan plan(QueriedRelation queriedRelation) {
        WrappedRelation relation = new WrappedRelation(queriedRelation);
        LogicalPlan plan = new Collect(queriedRelation, relation.aggregationSources(), relation.where());

        if (relation.aggregates().isPresent() && !relation.groupKeys().isPresent()) {
            plan = new HashAggregate(plan, relation.aggregates().orElse(Collections.emptyList()));
        }
        // TODO: groupingHashAggregate
        if (relation.having().isPresent()) {
            plan = new Filter(plan, relation.having().get());
        }
        if (relation.orderBy().isPresent()) {
            plan = new Order(plan, relation.orderBy().get());
        }
        if (relation.limit().isPresent()) {
            plan = new Limit(plan, relation.limit().get(), relation.offset().orElse(Literal.of(0L)));
        }
        return plan;
    }

    private class HashAggregate implements LogicalPlan {

        private final LogicalPlan source;
        private final List<Function> aggregates;

        HashAggregate(LogicalPlan source, List<Function> aggregates) {
            this.source = source;
            this.aggregates = aggregates;
        }

        @Override
        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 @Nullable OrderBy order) {
            Plan plan = source.makePhysical(plannerContext, projectionBuilder, NO_LIMIT, 0, null);

            if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {
                AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                    source.outputs(),
                    aggregates,
                    AggregateMode.ITER_FINAL,
                    RowGranularity.NODE
                );
                plan.addProjection(fullAggregation, null, null, null);
                return plan;
            }
            return null;
        }

        @Override
        public LogicalPlan tryCollapse() {
            LogicalPlan collapsed = source.tryCollapse();
            if (collapsed == source) {
                return this;
            }
            return new HashAggregate(collapsed, aggregates);
        }

        @Override
        public List<Symbol> outputs() {
            return new ArrayList<>(aggregates);
        }
    }

    static class Order implements LogicalPlan {

        private final LogicalPlan source;
        private final OrderBy orderBy;

        Order(LogicalPlan source, OrderBy orderBy) {
            this.source = source;
            this.orderBy = orderBy;
        }

        @Override
        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 OrderBy order) {
            Plan plan = source.makePhysical(plannerContext, projectionBuilder, limitHint, offset, order);
            OrderedTopNProjection orderedTopNProjection = new OrderedTopNProjection(
                limitHint,
                offset,
                source.outputs(),
                orderBy.orderBySymbols(),
                orderBy.reverseFlags(),
                orderBy.nullsFirst()
            );
            plan.addProjection(orderedTopNProjection, null, null, null);
            return plan;
        }

        @Override
        public LogicalPlan tryCollapse() {
            LogicalPlan collapsed = source.tryCollapse();
            if (collapsed == source) {
                return this;
            }
            return new Order(collapsed, orderBy);
        }

        @Override
        public List<Symbol> outputs() {
            return source.outputs();
        }
    }

    static class Collect implements LogicalPlan {

        private final QueriedRelation relation;
        private final List<Symbol> toCollect;
        private final WhereClause where;

        Collect(QueriedRelation relation, List<Symbol> toCollect, WhereClause where) {
            this.relation = relation;
            this.toCollect = toCollect;
            this.where = where;
        }

        @Override
        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 @Nullable OrderBy order) {
            if (relation instanceof QueriedTableRelation) {
                QueriedTableRelation rel = (QueriedTableRelation) this.relation;
                TableInfo tableInfo = rel.tableRelation().tableInfo();
                SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
                RoutedCollectPhase collectPhase = new RoutedCollectPhase(
                    plannerContext.jobId(),
                    plannerContext.nextExecutionPhaseId(),
                    "collect",
                    plannerContext.allocateRouting(
                        tableInfo,
                        where,
                        null,
                        sessionContext),
                    tableInfo.rowGranularity(),
                    toCollect,
                    Collections.emptyList(),
                    where,
                    DistributionInfo.DEFAULT_BROADCAST,
                    sessionContext.user()
                );
                return new io.crate.planner.node.dql.Collect(
                    collectPhase,
                    limitHint,
                    offset,
                    toCollect.size(),
                    limitHint,
                    null
                );
            }
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return toCollect;
        }
    }

    private class Filter implements LogicalPlan {

        private final LogicalPlan source;
        private final HavingClause havingClause;

        Filter(LogicalPlan source, HavingClause havingClause) {
            this.source = source;
            this.havingClause = havingClause;
        }

        @Override
        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 OrderBy order) {
            // TODO:
            return source.makePhysical(plannerContext, projectionBuilder, limitHint, offset, order);
        }

        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return source.outputs();
        }
    }

    private class Limit implements LogicalPlan {

        private final LogicalPlan source;
        private final Symbol limit;
        private final Symbol offset;

        Limit(LogicalPlan source, Symbol limit, Symbol offset) {
            this.source = source;
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 @Nullable OrderBy order) {
            // TODO:
            return source.makePhysical(plannerContext, projectionBuilder, limitHint, offset, order);
        }

        @Override
        public LogicalPlan tryCollapse() {
            LogicalPlan collapsed = source.tryCollapse();
            if (collapsed instanceof Order) {
                Order order = (Order) collapsed;
                return new TopN(order.source, order.orderBy, limit, offset).tryCollapse();
            }
            if (collapsed == source) {
                return this;
            }
            return new Limit(collapsed, limit, offset);
        }

        @Override
        public List<Symbol> outputs() {
            return source.outputs();
        }
    }

    private class TopN implements LogicalPlan {

        private final LogicalPlan source;
        private final OrderBy orderBy;
        private final Symbol limit;
        private final Symbol offset;

        public TopN(LogicalPlan source, OrderBy orderBy, Symbol limit, Symbol offset) {
            this.source = source;
            this.orderBy = orderBy;
            this.limit = limit;
            this.offset = offset;
        }

        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 @Nullable OrderBy order) {
            // TODO:
            return source.makePhysical(plannerContext, projectionBuilder, limitHint, offset, order);
        }

        @Override
        public LogicalPlan tryCollapse() {
            LogicalPlan collapsed = source.tryCollapse();
            if (collapsed instanceof Collect) {
                Collect collect = (Collect) collapsed;
                return new TopNCollect(
                    collect.relation,
                    collect.toCollect,
                    collect.where,
                    orderBy,
                    limit,
                    offset
                ).tryCollapse();
            }
            if (collapsed == source) {
                return this;
            }
            return new TopN(collapsed, orderBy, limit, offset);
        }

        @Override
        public List<Symbol> outputs() {
            return source.outputs();
        }
    }

    private class TopNCollect implements LogicalPlan {

        private final QueriedRelation relation;
        private final List<Symbol> toCollect;
        private final WhereClause where;
        private final OrderBy orderBy;
        private final Symbol limit;
        private final Symbol offset;

        TopNCollect(QueriedRelation relation,
                    List<Symbol> toCollect,
                    WhereClause where,
                    OrderBy orderBy,
                    Symbol limit,
                    Symbol offset) {
            this.relation = relation;
            this.toCollect = toCollect;
            this.where = where;
            this.orderBy = orderBy;
            this.limit = limit;
            this.offset = offset;
        }

        public Plan makePhysical(Planner.Context plannerContext,
                                 ProjectionBuilder projectionBuilder,
                                 int limitHint,
                                 int offset,
                                 @Nullable OrderBy order) {

            throw new UnsupportedOperationException("NYI");
        }


        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return toCollect;
        }
    }
}
