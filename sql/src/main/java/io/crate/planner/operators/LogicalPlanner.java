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

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QueryClause;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.RefVisitor;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.planner.MultiPhasePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.projection.builder.SplitPoints;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.firstNonNull;

public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    public Plan plan(QueriedRelation queriedRelation,
                     Planner.Context plannerContext,
                     ProjectionBuilder projectionBuilder) {
        LogicalPlan logicalPlan = plan(queriedRelation)
            .build(extractColumns(queriedRelation.querySpec().outputs()))
            .tryCollapse();

        Plan plan = logicalPlan.build(
            plannerContext,
            projectionBuilder,
            LogicalPlanner.NO_LIMIT,
            0,
            null
        );
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(plannerContext);
        return MultiPhasePlan.createIfNeeded(plan, subqueryPlanner.planSubQueries(queriedRelation.querySpec()));
    }

    static LogicalPlan.Builder plan(QueriedRelation relation) {
        SplitPoints splitPoints = SplitPoints.create(relation.querySpec());
        return fetchOrEval(
            limit(
                orderBy(
                    filter(
                        groupByOrAggregate(
                            whereAndCollect(relation, splitPoints.toCollect(), relation.where()),
                            relation.groupBy(),
                            splitPoints.aggregates()),
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

    private static LogicalPlan.Builder limit(LogicalPlan.Builder source, @Nullable Symbol limit, @Nullable Symbol offset) {
        if (limit == null && offset == null) {
            return source;
        }
        return usedColumns -> new Limit(
            source.build(usedColumns),
            firstNonNull(limit, Literal.of(-1L)),
            firstNonNull(offset, Literal.of(0L))
        );
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

    private static LogicalPlan.Builder groupByOrAggregate(LogicalPlan.Builder source,
                                                          List<Symbol> groupKeys,
                                                          List<Function> aggregates) {
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

    private static LogicalPlan.Builder whereAndCollect(QueriedRelation queriedRelation, List<Symbol> toCollect, WhereClause where) {
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

    private static LogicalPlan.Builder createCollect(QueriedTableRelation relation, List<Symbol> toCollect, WhereClause where) {
        return usedColumns -> new Collect(relation, toCollect, where, usedColumns);
    }

    private static LogicalPlan.Builder createJoinNodes(MultiSourceSelect mss, WhereClause where) {
        throw new UnsupportedOperationException("NYI createJoinNodes");
    }

    private static LogicalPlan.Builder filter(LogicalPlan.Builder sourceBuilder, @Nullable QueryClause queryClause) {
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

    private static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }
}
