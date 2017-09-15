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

package io.crate.planner;

import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.metadata.Functions;
import io.crate.planner.operators.LogicalPlanner;
import io.crate.planner.projection.builder.ProjectionBuilder;

class SelectStatementPlanner {

    private final Visitor visitor;

    SelectStatementPlanner(Functions functions) {
        visitor = new Visitor(functions);
    }

    public Plan plan(SelectAnalyzedStatement statement, Planner.Context context) {
        return visitor.process(statement.relation(), context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<Planner.Context, Plan> {

        private final Functions functions;

        public Visitor(Functions functions) {
            this.functions = functions;
        }

        @Override
        protected Plan visitAnalyzedRelation(AnalyzedRelation relation, Planner.Context context) {
            if (relation instanceof QueriedRelation) {
                QueriedRelation queriedRelation = (QueriedRelation) relation;
                context.applySoftLimit(queriedRelation.querySpec());
                LogicalPlanner logicalPlanner = new LogicalPlanner();
                return Merge.ensureOnHandler(
                    logicalPlanner.plan(queriedRelation, context, new ProjectionBuilder(functions)),
                    context
                );
            }
            throw new UnsupportedOperationException("Cannot create plan for: " + relation);
        }
    }
}
