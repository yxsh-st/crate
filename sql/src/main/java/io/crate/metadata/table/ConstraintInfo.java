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

package io.crate.metadata.table;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import org.apache.lucene.util.BytesRef;

public class ConstraintInfo {

    public enum Constraint {
        PRIMARY_KEY("PRIMARY_KEY"),
        NOT_NULL("NOT_NULL");

        private final String text;

        Constraint(final String text) {
            this.text = text;
        }

        public BytesRef getReference() {
            return new BytesRef(this.text);
        }
    }

    private final TableIdent tableIdent;
    private final ColumnIdent columnIdent;
    private final Constraint constraintType;

    public ConstraintInfo(TableIdent tableIdent, ColumnIdent columnIdent, Constraint constraintType) {
        this.tableIdent = tableIdent;
        this.columnIdent = columnIdent;
        this.constraintType = constraintType;
    }

    public TableIdent tableIdent() {
        return this.tableIdent;
    }

    public ColumnIdent columnIdent() {
        return this.columnIdent;
    }

    public BytesRef constraintType() {
        return this.constraintType.getReference();
    }
}
