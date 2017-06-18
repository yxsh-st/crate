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

package io.crate.operation.projectors;

import io.crate.action.FutureActionListener;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowBridging;
import io.crate.executor.transport.ShardRequest;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesRequest;
import org.elasticsearch.action.admin.indices.create.BulkCreateIndicesResponse;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class RowToBulkReq<TReq extends ShardRequest<TReq, TItem>, TItem extends ShardRequest.Item>
    implements AsyncProducer<Map<RowToBulkReq.ShardLocation, TReq>> {

    private final static Logger LOGGER = Loggers.getLogger(RowToBulkReq.class);

    private final UUID jobId;
    private final ClusterService clusterService;
    private final BatchIterator batchIterator;
    private final RowShardResolver rowShardResolver;
    private final Map<ShardLocation, TReq> requestsByLocation = new HashMap<>();
    private final Map<String, List<PendingRequest<TItem>>> pendingRequestsByIndex = new HashMap<>();
    private final Row row;
    private final Function<String, TItem> itemFactory;
    private final BiFunction<ShardId, String, TReq> requestFactory;
    private final Supplier<String> indexNameResolver;
    private final int bulkSize;
    private final boolean autoCreateIndices;
    private final List<? extends CollectExpression<Row, ?>> expressions;
    private final TransportBulkCreateIndicesAction createIndicesAction;

    private int numItems = -1;

    public RowToBulkReq(UUID jobId,
                        ClusterService clusterService,
                        BatchIterator batchIterator,
                        RowShardResolver rowShardResolver,
                        Function<String, TItem> itemFactory,
                        BiFunction<ShardId, String, TReq> requestFactory,
                        Supplier<String> indexNameResolver,
                        int bulkSize,
                        boolean autoCreateIndices,
                        List<? extends CollectExpression<Row, ?>> expressions,
                        TransportBulkCreateIndicesAction createIndicesAction) {
        this.jobId = jobId;
        this.clusterService = clusterService;
        this.batchIterator = batchIterator;
        this.rowShardResolver = rowShardResolver;
        this.row = RowBridging.toRow(batchIterator.rowData());
        this.itemFactory = itemFactory;
        this.requestFactory = requestFactory;
        this.indexNameResolver = indexNameResolver;
        this.bulkSize = bulkSize;
        this.autoCreateIndices = autoCreateIndices;
        this.expressions = expressions;
        this.createIndicesAction = createIndicesAction;
    }

    @Override
    public Map<ShardLocation, TReq> currentItem() {
        return requestsByLocation;
    }

    @Override
    public boolean moveNext() {
        while (batchIterator.moveNext()) {
            numItems++;
            rowShardResolver.setNextRow(row);
            for (int i = 0; i < expressions.size(); i++) {
                expressions.get(i).setNextRow(row);
            }
            TItem item = itemFactory.apply(rowShardResolver.id());
            String indexName = indexNameResolver.get();
            String routing = rowShardResolver.routing();
            ShardLocation shardLocation = getShardLocation(indexName, rowShardResolver.id(), routing);
            if (shardLocation == null) {
                addToPendingRequests(item, indexName, routing);
            } else {
                addToRequest(item, shardLocation, routing);
            }

            if (numItems % bulkSize == 0) {
                return true;
            }
        }
        return false;
    }

    private void addToRequest(TItem item, ShardLocation shardLocation, String routing) {
        TReq req = requestsByLocation.get(shardLocation);
        if (req == null) {
            req = requestFactory.apply(shardLocation.shardId, routing);
            requestsByLocation.put(shardLocation, req);
        }
        req.add(numItems, item);
    }

    private void addToPendingRequests(TItem item, String indexName, String routing) {
        List<PendingRequest<TItem>> pendingRequests = pendingRequestsByIndex.get(indexName);
        if (pendingRequests == null) {
            pendingRequests = new ArrayList<>();
            pendingRequestsByIndex.put(indexName, pendingRequests);
        }
        pendingRequests.add(new PendingRequest<>(item, routing));
    }

    @Override
    public CompletionStage<AsyncProducer<Map<ShardLocation, TReq>>> loadNextBatch() {
        if (numItems % bulkSize == 0) {
            FutureActionListener<BulkCreateIndicesResponse, BulkCreateIndicesResponse> listener =
                new FutureActionListener<>(Function.identity());
            createIndicesAction.execute(new BulkCreateIndicesRequest(pendingRequestsByIndex.keySet(), jobId), listener);
            return listener.thenApply(r -> this);
        }
        return batchIterator.loadNextBatch().thenApply(r -> this);
    }

    @Override
    public boolean allLoaded() {
        return pendingRequestsByIndex.isEmpty() && batchIterator.allLoaded();
    }

    @Nullable
    private ShardLocation getShardLocation(String indexName, String id, @Nullable String routing) {
        try {
            ShardIterator shardIterator = clusterService.operationRouting().indexShards(
                clusterService.state(),
                indexName,
                id,
                routing
            );

            String nodeId;
            ShardRouting shardRouting = shardIterator.nextOrNull();
            if (shardRouting == null) {
                nodeId = null;
            } else if (shardRouting.active() == false) {
                nodeId = shardRouting.relocatingNodeId();
            } else {
                nodeId = shardRouting.currentNodeId();
            }

            if (nodeId == null && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Unable to get the node id for index {} and shard {}", indexName, id);
            }
            return new ShardLocation(shardIterator.shardId(), nodeId);
        } catch (IndexNotFoundException e) {
            if (!autoCreateIndices) {
                throw e;
            }
            return null;
        }
    }

    private static class PendingRequest<TItem> {
        private final TItem item;
        private final String routing;

        PendingRequest(TItem item, String routing) {
            this.item = item;
            this.routing = routing;
        }
    }

    static class ShardLocation {
        private final ShardId shardId;
        private final String nodeId;

        private ShardLocation(ShardId shardId, String nodeId) {
            this.shardId = shardId;
            this.nodeId = nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ShardLocation that = (ShardLocation) o;

            if (!shardId.equals(that.shardId)) return false;
            return nodeId != null ? nodeId.equals(that.nodeId) : that.nodeId == null;
        }

        @Override
        public int hashCode() {
            int result = shardId.hashCode();
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            return result;
        }
    }
}
