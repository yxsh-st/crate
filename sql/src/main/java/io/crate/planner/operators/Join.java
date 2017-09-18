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
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class Join implements LogicalPlan {

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
        Plan left = lhs.build(plannerContext, projectionBuilder, LogicalPlanner.NO_LIMIT, 0, null);
        Plan right = rhs.build(plannerContext, projectionBuilder, LogicalPlanner.NO_LIMIT, 0, null);

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
        if (resultDescription.hasUnAppliedLimit()) {
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

    @Override
    public LogicalPlan tryCollapse() {
        return this;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }
}
