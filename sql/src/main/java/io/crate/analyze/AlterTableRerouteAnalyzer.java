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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.RerouteAllocateReplicaShard;
import io.crate.sql.tree.RerouteCancelShard;
import io.crate.sql.tree.RerouteMoveShard;
import io.crate.sql.tree.RerouteRetryFailed;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class AlterTableRerouteAnalyzer {

    private final Schemas schemas;
    private static final RerouteOptionVisitor REROUTE_OPTION_VISITOR = new RerouteOptionVisitor();

    AlterTableRerouteAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AnalyzedStatement analyze(AlterTableReroute node, Analysis context) {
        TableInfo tableInfo = schemas.getTableInfo(
            TableIdent.of(node.table(), context.sessionContext().defaultSchema()),
            Operation.ALTER);
        Row parameters = context.parameterContext().parameters();
        PartitionName partitionName = AlterTableAnalyzer.createPartitionName(node.table().partitionProperties(),
            (DocTableInfo) tableInfo, parameters);
        return REROUTE_OPTION_VISITOR.process(node.rerouteOption(), new Context(tableInfo, partitionName, parameters));
    }

    private class Context {

        final TableInfo tableInfo;
        private PartitionName partitionName;
        final Row params;

        private Context(TableInfo tableInfo, PartitionName partitionName, Row params) {
            this.tableInfo = tableInfo;
            this.partitionName = partitionName;
            this.params = params;
        }

    }

    private static class RerouteOptionVisitor extends AstVisitor<RerouteAnalyzedStatement, Context> {
        @Override
        public RerouteAnalyzedStatement visitRerouteMoveShard(RerouteMoveShard node, Context context) {
            int shardId = ExpressionToNumberVisitor.convert(node.shardId(), context.params).intValue();
            String fromNodeId = ExpressionToStringVisitor.convert(node.fromNodeId(), context.params);
            String toNodeId = ExpressionToStringVisitor.convert(node.toNodeId(), context.params);
            return new RerouteMoveShardAnalyzedStatement(context.tableInfo, context.partitionName,
                shardId, fromNodeId, toNodeId);
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteCancelShard(RerouteCancelShard node, Context context) {
            int shardId = ExpressionToNumberVisitor.convert(node.shardId(), context.params).intValue();
            String nodeId = ExpressionToStringVisitor.convert(node.nodeId(), context.params);
            Settings.Builder builder = Settings.builder();
            if (node.properties().isPresent()) {
                for (Map.Entry<String, Expression> entry : node.properties().get().properties().entrySet()) {
                    GenericPropertiesConverter.genericPropertyToSetting(builder,
                        entry.getKey(), entry.getValue(), context.params);
                }
            }
            return new RerouteCancelShardAnalyzedStatement(context.tableInfo, context.partitionName,
                shardId, nodeId, builder.build());
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard node, Context context) {
            int shardId = ExpressionToNumberVisitor.convert(node.shardId(), context.params).intValue();
            String nodeId = ExpressionToStringVisitor.convert(node.nodeId(), context.params);
            return new RerouteAllocateReplicaShardAnalyzedStatement(context.tableInfo, context.partitionName,
                shardId, nodeId);
        }

        @Override
        public RerouteAnalyzedStatement visitRerouteRetryFailed(RerouteRetryFailed node, Context context) {
            return new RerouteRetryFailedAnalyzedStatement(context.tableInfo, context.partitionName);
        }
    }
}
