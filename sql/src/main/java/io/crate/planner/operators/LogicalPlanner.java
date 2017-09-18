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

package io.crate.planner.operators;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.HavingClause;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QueryClause;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.fetch.FetchRewriter;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.PlanWithFetchDescription;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;

public class LogicalPlanner {

    public static final int NO_LIMIT = -1;


    /**
     * LogicalPlan is a tree of "Operators"
     * This is a representation of the logical order of operators that need to be executed to produce a correct result.
     *
     * {@link #build(Planner.Context, ProjectionBuilder, int, int, OrderBy)} is used to create the
     * actual "physical" execution plan.
     *
     * A Operator is something like Limit, OrderBy, HashAggregate, Join, Union, Collect
     * <pre>
     *     select x, y, z from t1 where x = 10 order by x limit 10:
     *
     *     Limit 10
     *        |
     *     Order By x
     *         |
     *     Collect [x, y, z]
     * </pre>
     *
     * {@link #build(Planner.Context, ProjectionBuilder, int, int, OrderBy)} is called on the "root" and flows down.
     * Each time each operator may provide "hints" to the children so that they can decide to eagerly apply parts of the
     * operations
     *
     * This allows us to create execution plans as follows::
     *
     * <pre>
     *     select x, y, z from t1 where order by x limit 10;
     *
     *
     *          Merge
     *         /  limit10
     *       /         \
     *     Collect     Collect
     *     limit 10    limit 10
     *
     * </pre>
     */
    public interface LogicalPlan {

        interface Builder {

            /**
             * Create a LogicalPlan node
             *
             * @param usedColumns The columns the "parent" is using.
             *                    This is used to create plans which utilize query-then-fetch.
             *                    For example:
             *                    <pre>
             *                       select a, b, c from t1 order by a limit 10
             *
             *                       EvalFetch (usedColumns: [a, b, c])
             *                         outputs: [a, b, c]   (b, c resolved using _fetch)
             *                         |
             *                       Limit 10 (usedColumns: [])
             *                         |
             *                       Order (usedColumns: [a]
             *                         |
             *                       Collect (usedColumns: [a] - inherited from Order)
             *                         outputs: [_fetch, a]
             *                    </pre>
             */
            LogicalPlan build(Set<Symbol> usedColumns);
        }

        // TODO: describe limit/offset/orderBy
        Plan build(Planner.Context plannerContext,
                   ProjectionBuilder projectionBuilder,
                   int limitHint,
                   int offset,
                   @Nullable OrderBy order);

        /**
         * Used to generate optimized operators.
         * E.g. Aggregate(count(*)) + Collect -> Count
         */
        LogicalPlan tryCollapse();

        List<Symbol> outputs();
    }

    public Plan plan(QueriedRelation queriedRelation,
                     Planner.Context plannerContext,
                     ProjectionBuilder projectionBuilder) {
        LogicalPlanner.LogicalPlan logicalPlan = plan(queriedRelation)
            .build(extractColumns(queriedRelation.querySpec().outputs()))
            .tryCollapse();

        return logicalPlan.build(
            plannerContext,
            projectionBuilder,
            LogicalPlanner.NO_LIMIT,
            0,
            null
        );
    }

    private LogicalPlan.Builder plan(QueriedRelation queriedRelation) {
        WrappedRelation relation = new WrappedRelation(queriedRelation);
        return fetchOrEval(
            limit(
                orderBy(
                    filter(
                        groupByOrAggregate(
                            whereAndCollect(queriedRelation, relation.toCollect(), relation.where()),
                            relation.groupKeys(),
                            relation.aggregates()
                        ),
                        relation.having()
                    ),
                    relation.orderBy()
                ),
                relation.limit(),
                relation.offset()
            ),
            relation.querySpec().outputs()
        );
    }

    private static LogicalPlan.Builder fetchOrEval(LogicalPlan.Builder source, List<Symbol> outputs) {
        return usedColumns -> new FetchOrEval(source.build(Collections.emptySet()), usedColumns, outputs);
    }

    private static LogicalPlan.Builder limit(LogicalPlan.Builder source, Optional<Symbol> limit, Optional<Symbol> offset) {
        if (!limit.isPresent()) {
            return source;
        }
        return usedColumns -> new Limit(source.build(usedColumns), limit.get(), offset.orElse(Literal.of(0L)));
    }

    private static LogicalPlan.Builder orderBy(LogicalPlan.Builder source, @Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return source;
        }
        Set<Symbol> columnsInOrderBy = extractColumns(orderBy.orderBySymbols());
        return usedColumns -> {
            columnsInOrderBy.addAll(usedColumns);
            return new Order(source.build(columnsInOrderBy), orderBy);
        };
    }

    private static LogicalPlan.Builder groupByOrAggregate(LogicalPlan.Builder source, List<Symbol> groupKeys, List<Function> aggregates) {
        if (!groupKeys.isEmpty()) {
            return usedColumns -> new GroupHashAggregate(
                source.build(extractColumns(Lists2.concat(groupKeys, aggregates))),
                aggregates,
                groupKeys
            );
        }
        if (!aggregates.isEmpty()) {
            return usedColumns -> new HashAggregate(source.build(extractColumns(aggregates)), aggregates);
        }
        return source;
    }

    private LogicalPlan.Builder whereAndCollect(QueriedRelation queriedRelation, List<Symbol> toCollect, WhereClause where) {
        if (queriedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation selectRelation = (QueriedSelectRelation) queriedRelation;
            return filter(plan(selectRelation.subRelation()), where);
        }
        if (queriedRelation instanceof MultiSourceSelect) {
            return createJoinNodes(((MultiSourceSelect) queriedRelation), where);
        }
        if (queriedRelation instanceof QueriedTableRelation) {
            return createCollect((QueriedTableRelation) queriedRelation, toCollect, where);
        }
        throw new UnsupportedOperationException("Cannot create LogicalPlan from: " + queriedRelation);
    }

    private LogicalPlan.Builder createCollect(QueriedTableRelation relation, List<Symbol> toCollect, WhereClause where) {
        return usedColumns -> new Collect(relation, toCollect, where, usedColumns);
    }

    private LogicalPlan.Builder createJoinNodes(MultiSourceSelect mss, WhereClause where) {
        throw new UnsupportedOperationException("NYI createJoinNodes");
    }

    private LogicalPlan.Builder filter(LogicalPlan.Builder sourceBuilder, @Nullable QueryClause queryClause) {
        if (queryClause == null) {
            return sourceBuilder;
        }
        if (queryClause.hasQuery()) {
            Set<Symbol> columnsInQuery = extractColumns(queryClause.query());
            return usedColumns -> {
                columnsInQuery.addAll(usedColumns);
                return new Filter(sourceBuilder.build(columnsInQuery), queryClause);
            };
        }
        if (queryClause.noMatch()) {
            return usedColumns -> new Filter(sourceBuilder.build(usedColumns), WhereClause.NO_MATCH);
        }
        return sourceBuilder;
    }

    private static class HashAggregate implements LogicalPlan {

        private final LogicalPlan source;
        private final List<Function> aggregates;

        HashAggregate(LogicalPlan source, List<Function> aggregates) {
            this.source = source;
            this.aggregates = aggregates;
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order) {


            Plan plan = source.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null);

            if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {
                AggregationProjection fullAggregation = projectionBuilder.aggregationProjection(
                    source.outputs(),
                    aggregates,
                    AggregateMode.ITER_FINAL,
                    RowGranularity.CLUSTER
                );
                plan.addProjection(fullAggregation, null, null, null);
                return plan;
            }

            AggregationProjection toPartial = projectionBuilder.aggregationProjection(
                source.outputs(),
                aggregates,
                AggregateMode.ITER_PARTIAL,
                RowGranularity.CLUSTER
            );
            plan.addProjection(toPartial, null, null, null);

            AggregationProjection toFinal = projectionBuilder.aggregationProjection(
                aggregates,
                aggregates,
                AggregateMode.PARTIAL_FINAL,
                RowGranularity.CLUSTER
            );
            return new Merge(
                plan,
                new MergePhase(
                    plannerContext.jobId(),
                    plannerContext.nextExecutionPhaseId(),
                    "mergeOnHandler",
                    plan.resultDescription().nodeIds().size(),
                    Collections.singletonList(plannerContext.handlerNode()),
                    plan.resultDescription().streamOutputs(),
                    Collections.singletonList(toFinal),
                    DistributionInfo.DEFAULT_SAME_NODE,
                    null
                ),
                NO_LIMIT,
                0,
                aggregates.size(),
                1,
                null
            );
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
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          OrderBy order) {
            Plan plan = source.build(plannerContext, projectionBuilder, limitHint, offset, orderBy);
            if (plan.resultDescription().orderBy() == null) {
                Projection projection = ProjectionBuilder.topNOrEval(
                    source.outputs(),
                    orderBy,
                    offset,
                    limitHint,
                    source.outputs()
                );
                plan.addProjection(projection, null, null, PositionalOrderBy.of(orderBy, source.outputs()));
            }
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
        private final WhereClause where;

        private final List<Symbol> toCollect;
        private final FetchRewriter.FetchDescription fetchDescription;
        private final TableInfo tableInfo;

        Collect(QueriedTableRelation relation, List<Symbol> toCollect, WhereClause where, Set<Symbol> usedColumns) {
            this.relation = relation;
            this.where = where;
            this.tableInfo = relation.tableRelation().tableInfo();

            // TODO: extract/rework this
            if (tableInfo instanceof DocTableInfo) {
                DocTableInfo docTableInfo = (DocTableInfo) this.tableInfo;
                Set<Symbol> columnsToCollect = extractColumns(toCollect);
                Sets.SetView<Symbol> unusedColumns = Sets.difference(columnsToCollect, usedColumns);
                ArrayList<Symbol> fetchable = new ArrayList<>();
                ArrayList<Reference> fetchRefs = new ArrayList<>();
                for (Symbol unusedColumn : unusedColumns) {
                    if (!Symbols.containsColumn(unusedColumn, DocSysColumns.SCORE)) {
                        fetchable.add(unusedColumn);
                    }
                    RefVisitor.visitRefs(unusedColumn, fetchRefs::add);
                }
                if (!fetchable.isEmpty()) {
                    Reference fetchIdRef = DocSysColumns.forTable(docTableInfo.ident(), DocSysColumns.FETCHID);
                    ArrayList<Symbol> preFetchSymbols = new ArrayList<>();
                    preFetchSymbols.add(fetchIdRef);
                    preFetchSymbols.addAll(usedColumns);
                    fetchDescription = new FetchRewriter.FetchDescription(
                        docTableInfo.ident(),
                        docTableInfo.partitionedByColumns(),
                        fetchIdRef,
                        preFetchSymbols,
                        toCollect,
                        fetchRefs
                    );
                    this.toCollect = preFetchSymbols;
                } else {
                    this.fetchDescription = null;
                    this.toCollect = toCollect;
                }
            } else {
                this.fetchDescription = null;
                this.toCollect = toCollect;
            }
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order) {
            QueriedTableRelation rel = (QueriedTableRelation) this.relation;
            // workaround for dealing with fields from joins
            java.util.function.Function<? super Symbol, ? extends Symbol> fieldsToRefs =
                FieldReplacer.bind(f -> rel.querySpec().outputs().get(f.index()));
            List<Symbol> collectRefs = Lists2.copyAndReplace(toCollect, fieldsToRefs);

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
                collectRefs,
                Collections.emptyList(),
                where,
                DistributionInfo.DEFAULT_BROADCAST,
                sessionContext.user()
            );
            collectPhase.orderBy(order);
            io.crate.planner.node.dql.Collect collect = new io.crate.planner.node.dql.Collect(
                collectPhase,
                limitHint,
                offset,
                collectRefs.size(),
                limitHint,
                PositionalOrderBy.of(order, collectRefs)
            );
            if (fetchDescription == null) {
                return collect;
            }
            return new PlanWithFetchDescription(collect, fetchDescription);
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

    private static class Filter implements LogicalPlan {

        private final LogicalPlan source;
        private final QueryClause queryClause;

        Filter(LogicalPlan source, QueryClause queryClause) {
            this.source = source;
            this.queryClause = queryClause;
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          OrderBy order) {
            Plan plan = source.build(plannerContext, projectionBuilder, limitHint, offset, order);
            FilterProjection filterProjection = ProjectionBuilder.filterProjection(source.outputs(), queryClause);
            filterProjection.requiredGranularity(RowGranularity.SHARD);
            plan.addProjection(filterProjection, null, null, null);
            return plan;
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

    private static class Limit implements LogicalPlan {

        private final LogicalPlan source;
        private final Symbol limit;
        private final Symbol offset;

        Limit(LogicalPlan source, Symbol limit, Symbol offset) {
            this.source = source;
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offsetHint,
                          @Nullable OrderBy order) {
            int limit = firstNonNull(plannerContext.toInteger(this.limit), NO_LIMIT);
            int offset = firstNonNull(plannerContext.toInteger(this.offset), 0);

            Plan plan = source.build(plannerContext, projectionBuilder, limit + offset, 0, order);
            List<Symbol> inputCols = InputColumn.fromSymbols(source.outputs());
            if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {
                plan.addProjection(new TopNProjection(limit, offset, inputCols), null, null, null);
            } else {
                plan.addProjection(new TopNProjection(limit + offset, 0, inputCols), limit, offset, null);
            }
            return plan;
        }

        @Override
        public LogicalPlan tryCollapse() {
            LogicalPlan collapsed = source.tryCollapse();
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

    private static class Join implements LogicalPlan {

        private final LogicalPlan lhs;
        private final LogicalPlan rhs;
        private final JoinPair joinPair;
        private final List<Symbol> outputs;

        Join(LogicalPlan lhs, LogicalPlan rhs, JoinPair joinPair) {
            this.lhs = lhs;
            this.rhs = rhs;
            this.joinPair = joinPair;
            // TODO: lhs outputs / rhs outputs may contain Refs, and everything above a Join is using Fields
            // this breaks everything
            // -> to fix this we'd have to delay the Field->Reference normalization
            // it Could be done within Collect - and only for the fields used in the CollectPhase
            this.outputs = Lists2.concat(lhs.outputs(), rhs.outputs());
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order) {

            // TODO: add columns from joinPair to usedColumns
            // TODO: extractColumns is a workaround to prevent fetch for now
            Plan left = lhs.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null);
            Plan right = rhs.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null);

            // TODO: distribution planning

            List<String> nlExecutionNodes = Collections.singletonList(plannerContext.handlerNode());
            NestedLoopPhase nlPhase = new NestedLoopPhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "nestedLoop",
                // NestedLoopPhase ctor want's at least one projection
                Collections.singletonList(new EvalProjection(InputColumn.fromSymbols(outputs))),
                receiveResultFrom(plannerContext, left.resultDescription(), nlExecutionNodes),
                receiveResultFrom(plannerContext, right.resultDescription(), nlExecutionNodes),
                nlExecutionNodes,
                joinPair.joinType(),
                joinPair.condition(),
                lhs.outputs().size(),
                rhs.outputs().size()
            );
            return new NestedLoop(
                nlPhase,
                left,
                right,
                limitHint,
                offset,
                limitHint,
                outputs.size(),
                null
            );
        }

        private static MergePhase receiveResultFrom(Planner.Context plannerContext,
                                             ResultDescription resultDescription,
                                             Collection<String> executionNodes) {
            final List<Projection> projections;
            if (hasUnAppliedLimit(resultDescription)) {
                projections = Collections.singletonList(ProjectionBuilder.topNOrEvalIfNeeded(
                    resultDescription.limit(),
                    resultDescription.offset(),
                    resultDescription.numOutputs(),
                    resultDescription.streamOutputs()
                ));
            } else {
                projections = Collections.emptyList();
            }
            return new MergePhase(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                "nl-receive-source-result",
                resultDescription.nodeIds().size(),
                executionNodes,
                resultDescription.streamOutputs(),
                projections,
                DistributionInfo.DEFAULT_SAME_NODE,
                resultDescription.orderBy()
            );
        }

        private static boolean hasUnAppliedLimit(ResultDescription resultDescription) {
            return resultDescription.limit() != NO_LIMIT || resultDescription.offset() != 0;
        }

        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return outputs;
        }
    }

    private static class GroupHashAggregate implements LogicalPlan {

        private final LogicalPlan source;
        private final List<Function> aggregates;
        private final List<Symbol> groupKeys;
        private final List<Symbol> outputs;

        GroupHashAggregate(LogicalPlan source, List<Function> aggregates, List<Symbol> groupKeys) {
            this.source = source;
            this.aggregates = aggregates;
            this.groupKeys = groupKeys;
            this.outputs = Lists2.concat(groupKeys, aggregates);
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order) {

            Plan plan = source.build(plannerContext, projectionBuilder, NO_LIMIT, 0, null);
            if (ExecutionPhases.executesOnHandler(plannerContext.handlerNode(), plan.resultDescription().nodeIds())) {
                GroupProjection groupProjection = projectionBuilder.groupProjection(
                    source.outputs(),
                    groupKeys,
                    aggregates,
                    AggregateMode.ITER_FINAL,
                    RowGranularity.CLUSTER
                );
                plan.addProjection(groupProjection, null, null, null);
                return plan;
            }
            GroupProjection toPartial = projectionBuilder.groupProjection(
                source.outputs(),
                groupKeys,
                aggregates,
                AggregateMode.ITER_PARTIAL,
                RowGranularity.SHARD
            );
            plan.addProjection(toPartial, null, null, null);

            GroupProjection toFinal = projectionBuilder.groupProjection(
                outputs,
                groupKeys,
                aggregates,
                AggregateMode.PARTIAL_FINAL,
                RowGranularity.CLUSTER
            );

            // TODO: Would need rowAuthority information on source to be able to optimize like ReduceOnCollectorGroupByConsumer
            // TODO: To decide if a re-distribution step is useful numExpectedRows/cardinality information would be great.

            return new Merge(
                plan,
                new MergePhase(
                    plannerContext.jobId(),
                    plannerContext.nextExecutionPhaseId(),
                    "mergeOnHandler",
                    plan.resultDescription().nodeIds().size(),
                    Collections.singletonList(plannerContext.handlerNode()),
                    plan.resultDescription().streamOutputs(),
                    Collections.singletonList(toFinal),
                    DistributionInfo.DEFAULT_SAME_NODE,
                    null
                ),
                NO_LIMIT,
                0,
                outputs.size(),
                NO_LIMIT,
                null
            );
        }

        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return outputs;
        }
    }

    private static class FetchOrEval implements LogicalPlan {

        private final LogicalPlan source;
        private final List<Symbol> outputs;

        FetchOrEval(LogicalPlan source, Set<Symbol> usedColumns, List<Symbol> outputs) {
            this.source = source;
            this.outputs = outputs;
        }

        @Override
        public Plan build(Planner.Context plannerContext,
                          ProjectionBuilder projectionBuilder,
                          int limitHint,
                          int offset,
                          @Nullable OrderBy order) {

            // TODO: make use of usedColumns to propagate fetch if the parent doesn't use the columns

            Plan plan = source.build(plannerContext, projectionBuilder, limitHint, offset, order);
            FetchRewriter.FetchDescription fetchDescription = plan.resultDescription().fetchDescription();
            if (fetchDescription == null) {
                InputColumns.Context ctx = new InputColumns.Context(source.outputs());
                plan.addProjection(new EvalProjection(InputColumns.create(outputs, ctx)), null, null, null);
                return plan;
            }
            if (plan instanceof PlanWithFetchDescription) {
                plan = ((PlanWithFetchDescription) plan).subPlan();
            }
            plan = Merge.ensureOnHandler(plan, plannerContext);
            // TODO: change how this is created (utilize outputs)
            ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
            FetchPhase fetchPhase = new FetchPhase(
                plannerContext.nextExecutionPhaseId(),
                readerAllocations.nodeReaders().keySet(),
                readerAllocations.bases(),
                readerAllocations.tableIndices(),
                fetchDescription.fetchRefs()
            );
            InputColumn fetchId = new InputColumn(0);
            FetchSource fetchSource = new FetchSource(
                fetchDescription.partitionedByColumns(),
                Collections.singletonList(fetchId),
                fetchDescription.fetchRefs()
            );
            FetchProjection fetchProjection = new FetchProjection(
                fetchPhase.phaseId(),
                plannerContext.fetchSize(),
                ImmutableMap.of(fetchDescription.table(), fetchSource),
                FetchRewriter.generateFetchOutputs(fetchDescription),
                readerAllocations.nodeReaders(),
                readerAllocations.indices(),
                readerAllocations.indicesToIdents()
            );
            plan.addProjection(fetchProjection, null, null, null);
            InputColumns.Context ctx = new InputColumns.Context(fetchDescription.postFetchOutputs);
            List<Symbol> inputCols = InputColumns.create(outputs, ctx);
            plan.addProjection(new EvalProjection(inputCols), null, null, null);
            return new QueryThenFetch(plan, fetchPhase);
        }

        @Override
        public LogicalPlan tryCollapse() {
            return this;
        }

        @Override
        public List<Symbol> outputs() {
            return outputs;
        }
    }

    private static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    private static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }

    /*
    private static Collect createCollect(QueriedTableRelation relation, WhereClause where) {
        return new Collect(relation, where);
    }

    private static LogicalPlan createDataSource(QueriedRelation queriedRelation, List<Symbol> toCollect, WhereClause where) {
        if (queriedRelation instanceof QueriedTableRelation) {
            return new Collect(queriedRelation, toCollect, where);
        }
        if (queriedRelation instanceof MultiSourceSelect) {
            MultiSourceSelect mss = (MultiSourceSelect) queriedRelation;
            final Map<Set<QualifiedName>, Symbol> queryParts;
            if (where.hasQuery()) {
                queryParts = QuerySplitter.split(where.query());
            } else {
                queryParts = Collections.emptyMap();
            }
            // TODO: add relation-ordering-logic
            Iterator<AnalyzedRelation> iterator = mss.sources().values().iterator();

            AnalyzedRelation left = iterator.next();
            AnalyzedRelation right = iterator.next();
            LogicalPlan lhs = createCollect(left);
            LogicalPlan rhs = createCollect(right);

            JoinPair joinPair = JoinPairs.findAndRemovePair(
                mss.joinPairs(), left.getQualifiedName(), right.getQualifiedName());

            LogicalPlan plan = new Join(lhs, rhs, joinPair);
            // TODO: add a Filter for query
            HashSet<QualifiedName> relNames = Sets.newHashSet(left.getQualifiedName(), right.getQualifiedName());
            Symbol query = queryParts.get(relNames);

            while (iterator.hasNext()) {
                right = iterator.next();
                rhs = createCollect(right);
                joinPair = JoinPairs.findAndRemovePair(mss.joinPairs(), left.getQualifiedName(), right.getQualifiedName());

                plan = new Join(plan, rhs, joinPair);
            }
            return plan;
        }
        throw new UnsupportedOperationException("NYI: " + queriedRelation);
    }
    */

    interface NewQueriedRelation extends QueriedRelation {

        List<Symbol> outputs();

        WhereClause where();

        List<Symbol> groupKeys();

        @Nullable
        HavingClause having();

        @Nullable
        OrderBy orderBy();

        Optional<Symbol> limit();

        Optional<Symbol> offset();



        List<Function> aggregates();

        List<Symbol> toCollect();
    }

    static class WrappedRelation implements NewQueriedRelation {

        private final QueriedRelation relation;
        private final List<Function> aggregates;
        private final List<Symbol> toCollect;

        WrappedRelation(QueriedRelation relation) {
            this.relation = relation;
            SplitPoints splitPoints = SplitPoints.create(relation.querySpec());
            // TODO: Might make more sense to use the "usedSymbols" functionality - so that a parent ORDER operator
            // can indicate to the child that the orderBy symbols are used
            // Though they're *always* used - so doing it like this might be simpler
            if (splitPoints.aggregates().isEmpty()) {
                this.aggregates = Collections.emptyList();
                Optional<OrderBy> orderBy = relation.querySpec().orderBy();
                if (orderBy.isPresent() && !relation.querySpec().groupBy().isPresent()) {
                    this.toCollect = Lists2.concatUnique(splitPoints.toCollect(), orderBy.get().orderBySymbols());
                } else {
                    this.toCollect = splitPoints.toCollect();
                }
            } else {
                this.aggregates = splitPoints.aggregates();
                this.toCollect = splitPoints.toCollect();
            }
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
        public List<Symbol> groupKeys() {
            return relation.querySpec().groupBy().orElse(Collections.emptyList());
        }

        @Override
        public HavingClause having() {
            return relation.querySpec().having().orElse(null);
        }

        @Override
        public OrderBy orderBy() {
            return relation.querySpec().orderBy().orElse(null);
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
        public List<Function> aggregates() {
            return aggregates;
        }

        @Override
        public List<Symbol> toCollect() {
            return toCollect;
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
}
