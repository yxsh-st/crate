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

package io.crate.operation.collect.sources;

import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.BatchConsumer;
import io.crate.data.RowN;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.List;
import java.util.function.Function;

@Singleton
public class SingleRowSource implements CollectSource {

    private final Function<? super Symbol, Object> evaluator;

    @Inject
    public SingleRowSource(Functions functions, ClusterReferenceResolver clusterRefResolver) {
        InputFactory inputFactory = new InputFactory(functions);
        evaluator = inputFactory.evaluatorFor(clusterRefResolver);
    }

    @Override
    public CrateCollector getCollector(CollectPhase phase, BatchConsumer consumer, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        List<Symbol> toCollect = collectPhase.toCollect();
        WhereClause whereClause = collectPhase.whereClause();

        if (whereClause.hasQuery()) {
            Boolean match = (Boolean) evaluator.apply(whereClause.query());
            if (match == null || !match) {
                return RowsCollector.empty(consumer, toCollect.size());
            }
        } else if (whereClause.noMatch()) {
            return RowsCollector.empty(consumer, toCollect.size());
        }
        Object[] cells = new Object[toCollect.size()];
        for (int i = 0; i < toCollect.size(); i++) {
            cells[i] = evaluator.apply(toCollect.get(i));
        }
        return RowsCollector.single(new RowN(cells), consumer);
    }
}
