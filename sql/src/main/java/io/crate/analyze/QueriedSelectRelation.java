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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class QueriedSelectRelation implements QueriedRelation {

    private final Fields fields;
    private final QuerySpec querySpec;

    private AnalyzedRelation subRelation;

    public QueriedSelectRelation(AnalyzedRelation subRelation,
                                 Collection<? extends Path> outputNames) {
        this.subRelation = subRelation;
        this.querySpec = querySpec;
        this.fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
    }

    public QueriedRelation subRelation() {
        return subRelation;
    }

    public void subRelation(QueriedRelation subRelation) {
        this.subRelation = subRelation;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedSelectRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on QueriedSelectRelation is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return subRelation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        subRelation.setQualifiedName(qualifiedName);
    }

    @Override
    public List<Symbol> outputs() {
        return querySpec.outputs();
    }

    @Override
    public WhereClause where() {
        return querySpec.where();
    }

    @Override
    public Optional<List<Symbol>> groupBy() {
        return querySpec.groupBy();
    }

    @Override
    public Optional<HavingClause> having() {
        return querySpec.having();
    }

    @Override
    public Optional<OrderBy> orderBy() {
        return querySpec.orderBy();
    }

    @Override
    public Optional<Symbol> limit() {
        return querySpec.limit();
    }

    @Override
    public Optional<Symbol> offset() {
        return querySpec.offset();
    }
}
