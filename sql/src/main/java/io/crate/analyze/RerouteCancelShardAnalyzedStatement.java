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

import io.crate.metadata.PartitionName;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.common.settings.Settings;

public class RerouteCancelShardAnalyzedStatement extends RerouteAnalyzedStatement {

    private final int shardId;
    private final String fromNodeId;
    private final Settings toNodeId;

    public RerouteCancelShardAnalyzedStatement(TableInfo tableInfo,
                                               PartitionName partitionName,
                                               int shardId,
                                               String fromNodeId,
                                               Settings toNodeId) {
        super(tableInfo, partitionName);
        this.shardId = shardId;
        this.fromNodeId = fromNodeId;
        this.toNodeId = toNodeId;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitRerouteCancelShard(this, context);
    }
}
