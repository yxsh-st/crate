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

package io.crate.operation.collect;

import io.crate.data.RowConsumer;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;

public final class RowsCollector {

    public static CrateCollector empty(RowConsumer consumer) {
        return BatchIteratorCollectorBridge.newInstance(InMemoryBatchIterator.empty(), consumer);
    }

    public static CrateCollector single(Row row, RowConsumer consumer) {
        return BatchIteratorCollectorBridge.newInstance(InMemoryBatchIterator.newInstance(row), consumer);
    }

    public static CrateCollector forRows(Iterable<Row> rows, RowConsumer consumer) {
        return BatchIteratorCollectorBridge.newInstance(InMemoryBatchIterator.newInstance(rows), consumer);
    }

    static CrateCollector.Builder emptyBuilder() {
        return consumer -> BatchIteratorCollectorBridge.newInstance(InMemoryBatchIterator.empty(), consumer);
    }

    public static CrateCollector.Builder builder(final Iterable<Row> rows) {
        return batchConsumer -> BatchIteratorCollectorBridge.newInstance(
            InMemoryBatchIterator.newInstance(rows), batchConsumer);
    }
}
