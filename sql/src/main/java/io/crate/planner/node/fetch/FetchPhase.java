/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.node.fetch;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class FetchPhase implements ExecutionPhase {

    private final TreeMap<String, Integer> bases;
    private final Multimap<TableIdent, String> tableIndices;
    private final Collection<Reference> fetchRefs;

    private final int executionPhaseId;
    private final Set<String> executionNodes;

    public FetchPhase(int executionPhaseId,
                      Set<String> executionNodes,
                      TreeMap<String, Integer> bases,
                      Multimap<TableIdent, String> tableIndices,
                      Collection<Reference> fetchRefs) {
        this.executionPhaseId = executionPhaseId;
        this.executionNodes = executionNodes;
        this.bases = bases;
        this.tableIndices = tableIndices;
        this.fetchRefs = fetchRefs;
    }

    public Collection<Reference> fetchRefs() {
        return fetchRefs;
    }

    @Override
    public Type type() {
        return Type.FETCH;
    }

    @Override
    public String name() {
        return "fetchPhase";
    }

    @Override
    public int phaseId() {
        return executionPhaseId;
    }

    @Override
    public Set<String> nodeIds() {
        return executionNodes;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitFetchPhase(this, context);
    }

    public FetchPhase(StreamInput in) throws IOException {
        executionPhaseId = in.readVInt();

        int n = in.readVInt();
        executionNodes = new HashSet<>(n);
        for (int i = 0; i < n; i++) {
            executionNodes.add(in.readString());
        }

        n = in.readVInt();
        bases = new TreeMap<>();
        for (int i = 0; i < n; i++) {
            bases.put(in.readString(), in.readVInt());
        }

        n = in.readVInt();
        fetchRefs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            fetchRefs.add(Reference.fromStream(in));
        }

        n = in.readVInt();
        tableIndices = HashMultimap.create(n, 1);
        for (int i = 0; i < n; i++) {
            TableIdent ti = new TableIdent(in);
            int nn = in.readVInt();
            for (int j = 0; j < nn; j++) {
                tableIndices.put(ti, in.readString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(executionPhaseId);

        out.writeVInt(executionNodes.size());
        for (String executionNode : executionNodes) {
            out.writeString(executionNode);
        }

        out.writeVInt(bases.size());
        for (Map.Entry<String, Integer> entry : bases.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVInt(entry.getValue());
        }

        out.writeVInt(fetchRefs.size());
        for (Reference ref : fetchRefs) {
            Reference.toStream(ref, out);
        }
        Map<TableIdent, Collection<String>> map = tableIndices.asMap();
        out.writeVInt(map.size());
        for (Map.Entry<TableIdent, Collection<String>> entry : map.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeVInt(entry.getValue().size());
            for (String s : entry.getValue()) {
                out.writeString(s);
            }
        }

    }

    public Multimap<TableIdent, String> tableIndices() {
        return tableIndices;
    }

    public TreeMap<String, Integer> bases() {
        return bases;
    }
}
