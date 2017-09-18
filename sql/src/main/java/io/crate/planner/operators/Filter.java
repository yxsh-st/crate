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
import io.crate.analyze.QueryClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import java.util.List;

class Filter implements LogicalPlan {

    final LogicalPlan source;
    final QueryClause queryClause;

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
