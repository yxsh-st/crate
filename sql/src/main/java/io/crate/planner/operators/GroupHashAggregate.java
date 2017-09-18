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

import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.metadata.RowGranularity;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhases;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

class GroupHashAggregate implements LogicalPlan {

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

        Plan plan = source.build(plannerContext, projectionBuilder, LogicalPlanner.NO_LIMIT, 0, null);
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
            LogicalPlanner.NO_LIMIT,
            0,
            outputs.size(),
            LogicalPlanner.NO_LIMIT,
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
