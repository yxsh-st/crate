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
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.fetch.FetchRewriter;
import io.crate.planner.node.dql.PlanWithFetchDescription;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class FetchOrEval implements LogicalPlan {

    final LogicalPlan source;
    private final Set<Symbol> usedColumns;
    final List<Symbol> outputs;

    FetchOrEval(LogicalPlan source, Set<Symbol> usedColumns, List<Symbol> outputs) {
        this.source = source;
        this.usedColumns = usedColumns;
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
            if (!source.outputs().equals(outputs)) {
                InputColumns.Context ctx = new InputColumns.Context(source.outputs());
                plan.addProjection(new EvalProjection(InputColumns.create(outputs, ctx)), null, null, null);
            }
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
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == this) {
            return this;
        }
        return new FetchOrEval(collapsed, usedColumns, outputs);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
