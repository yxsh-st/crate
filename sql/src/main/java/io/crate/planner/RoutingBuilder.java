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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.WhereClause;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.fetch.IndexBaseVisitor;

import javax.annotation.Nullable;
import java.util.*;

final class RoutingBuilder {

    private final Multimap<TableIdent, TableRouting> tableRoutings = HashMultimap.create();

    //index, shardId, node
    private Map<String, Map<Integer, String>> shardNodes;
    private ReaderAllocations readerAllocations;
    private HashMultimap<TableIdent, String> tableIndices;

    @VisibleForTesting
    final static class TableRouting {
        final WhereClause where;
        final String preference;
        final Routing routing;
        boolean nodesAllocated = false;

        TableRouting(WhereClause where, String preference, Routing routing) {
            this.where = where;
            this.preference = preference;
            this.routing = routing;
        }
    }

    Routing allocateRouting(TableInfo tableInfo, WhereClause where, @Nullable String preference) {
        Collection<TableRouting> existingRoutings = tableRoutings.get(tableInfo.ident());
        // allocate routing nodes only if we have more than one table routings
        Routing routing;
        if (existingRoutings.isEmpty()) {
            routing = tableInfo.getRouting(where, preference);
            assert routing != null : tableInfo + " returned empty routing. Routing must not be null";
        } else {
            for (TableRouting existing : existingRoutings) {
                assert preference == null || preference.equals(existing.preference) :
                    "preference must not be null or equals existing preference";
                if (Objects.equals(existing.where, where)) {
                    return existing.routing;
                }
            }

            routing = tableInfo.getRouting(where, preference);
            // ensure all routings of this table are allocated
            // and update new routing by merging with existing ones
            for (TableRouting existingRouting : existingRoutings) {
                if (!existingRouting.nodesAllocated) {
                    allocateRoutingNodes(tableInfo.ident(), existingRouting.routing.locations());
                    existingRouting.nodesAllocated = true;
                }
                // Merge locations with existing routing
                routing.mergeLocations(existingRouting.routing.locations());
            }
            if (!allocateRoutingNodes(tableInfo.ident(), routing.locations())) {
                throw new UnsupportedOperationException(
                    "Nodes of existing routing are not allocated, routing rebuild needed");
            }
        }
        tableRoutings.put(tableInfo.ident(), new TableRouting(where, preference, routing));
        return routing;
    }

    ReaderAllocations buildReaderAllocations() {
        if (readerAllocations != null) {
            return readerAllocations;
        }

        IndexBaseVisitor visitor = new IndexBaseVisitor();

        // tableIdent -> indexName
        final Multimap<TableIdent, String> usedTableIndices = HashMultimap.create();
        for (final Map.Entry<TableIdent, Collection<TableRouting>> tableRoutingEntry : tableRoutings.asMap().entrySet()) {
            for (TableRouting tr : tableRoutingEntry.getValue()) {
                if (!tr.nodesAllocated) {
                    allocateRoutingNodes(tableRoutingEntry.getKey(), tr.routing.locations());
                    tr.nodesAllocated = true;
                }
                tr.routing.walkLocations(visitor);
                tr.routing.walkLocations(new Routing.RoutingLocationVisitor() {
                    @Override
                    public boolean visitNode(String nodeId, Map<String, List<Integer>> nodeRouting) {
                        usedTableIndices.putAll(tableRoutingEntry.getKey(), nodeRouting.keySet());
                        return super.visitNode(nodeId, nodeRouting);
                    }
                });

            }
        }
        readerAllocations = new ReaderAllocations(visitor.build(), shardNodes, usedTableIndices);
        return readerAllocations;
    }

    private boolean allocateRoutingNodes(TableIdent tableIdent, Map<String, Map<String, List<Integer>>> locations) {
        boolean success = true;
        if (tableIndices == null) {
            tableIndices = HashMultimap.create();
        }
        if (shardNodes == null) {
            shardNodes = new HashMap<>();
        }
        for (Map.Entry<String, Map<String, List<Integer>>> location : locations.entrySet()) {
            for (Map.Entry<String, List<Integer>> indexEntry : location.getValue().entrySet()) {
                Map<Integer, String> shardsOnIndex = shardNodes.get(indexEntry.getKey());
                tableIndices.put(tableIdent, indexEntry.getKey());
                List<Integer> shards = indexEntry.getValue();
                if (shardsOnIndex == null) {
                    shardsOnIndex = new HashMap<>(shards.size());
                    shardNodes.put(indexEntry.getKey(), shardsOnIndex);
                    for (Integer id : shards) {
                        shardsOnIndex.put(id, location.getKey());
                    }
                } else {
                    for (Integer id : shards) {
                        String allocatedNodeId = shardsOnIndex.get(id);
                        if (allocatedNodeId != null) {
                            if (!allocatedNodeId.equals(location.getKey())) {
                                success = false;
                            }
                        } else {
                            shardsOnIndex.put(id, location.getKey());
                        }
                    }
                }
            }
        }
        return success;
    }
}
