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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.FieldReplacer;
import io.crate.analyze.symbol.FieldsVisitor;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchPhase;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.planner.projection.EvalProjection;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The FetchOrEval operator is producing the values for all selected expressions.
 *
 * <p>
 * This can be a simple re-arranging of expressions, the evaluation of scalar functions or involve a fetch operation.
 * The first two are rather simple and are handled by a {@link EvalProjection}, the fetch operation is more complicated.
 * </p>
 *
 * <h2>Fetch</h2>:
 *
 * <p>
 * On user tables it's possible to collect a {@link DocSysColumns#FETCHID}, which is like a unique row-id, except that it
 * is only valid within an open index reader. This fetchId can be used to retrieve the values from a row.
 *
 * The idea of fetch is to avoid loading more values than required.
 * This is the case if the data is collected from several nodes and then merged on the coordinator while applying some
 * form of data reduction, like applying a limit.
 * </p>
 *
 * <pre>
 *     select a, b, c from t order by limit 500
 *
 *
 *       N1                 N2
 *        Collect 500        Collect 500
 *             \              /
 *              \            /
 *                Merge
 *              1000 -> 500
 *                  |
 *                Fetch 500
 * </pre>
 *
 * Examples:
 *
 * TODO: examples below are outdated - update before merging
 *
 * <pre>
 * ================================================================================
 * Simple select
 * ================================================================================
 *
 * select a, b, c from t1 order by a
 *
 * usedBeforeNextFetch: [a, b, c]
 * FetchOrEval
 *  |   toCollect:  [a, b, c]
 *  |   outputs:    [_fetchId] -> [a, b, c]
 *  |
 *  | usedBeforeNextFetch: []           # Rule: FetchOrEval can clear usedBeforeNextFetch
 * Order
 *  |   orderBy: [a]
 *  |
 *  | usedBeforeNextFetch: [a]
 * Collect
 *      toCollect: [a, b, c]
 *      outputs: [_fetchId, a]          # due to unused columns
 *
 *
 * ================================================================================
 * Select with aggregation on virtual table
 * ================================================================================
 *
 * select sum(a), sum(bb) from
 *      (select a, b + b as bb from t limit 500) tt
 *
 *  usedBeforeNextFetch: [sum(tt.a), sum(tt.bb)]
 *  FetchOrEval
 *   |  toCollect: [sum(tt.a), sum(tt.bb)]   // source.outputs matches -> FetchOrEval is noop
 *   |  outputs:   [sum(tt.a), sum(tt.bb)]
 *   |
 *   | usedBeforeNextFetch: []
 *  Aggregate
 *   | aggregates: sum(tt.a), sum(tt.bb)
 *   |
 *   | usedBeforeNextFetch: [tt.a, tt.bb]
 *  FetchOrEval
 *   |  toCollect:  [a, b + b]
 *   |  outputs:    [_fetchId] -> [a, b + b]
 *   |
 *   | usedBeforeNextFetch: []
 *  Limit 500
 *   |
 *   | usedBeforeNextFetch: []
 *  Collect
 *      toCollect:  [a, b + b]
 *      outputs:    [_fetchId]          # due to unused columns
 *
 * ================================================================================
 * Select on virtual table
 * ================================================================================
 *
 * select a, bb from
 *      (select a, b + b as bb from t) tt
 *
 * usedBeforeNextFetch: [tt.a, tt.bb]
 * FetchOrEval
 *  |   toCollect: [tt.a, tt.bb]
 *  |   outputs: [_fetchId] -> [tt.a, tt.bb] -> [t.a, t.b + t.b]
 *  |
 *  | usedBeforeNextFetch: []
 * FetchOrEval
 *  |   toCollect: [t.a, t.b + t.b]     unused: [t.a, t.b + t.b]
 *  |   outputs: [_fetchId]             # due to usedBeforeNextFetch being empty
 *  |
 * Collect
 *      toCollect: [t.a, t.b + t.b]
 *      outputs: [_fetchId]
 *
 * ================================================================================
 * Join
 * ================================================================================
 *
 * select tt1.x, tt2.x from
 *      (select x, id from t1) tt1
 *      inner join (select x, id from t2) tt2 on tt1.id = tt2.id
 *
 * usedBeforeNextFetch: [tt1.x, tt2.x]
 * FetchOrEval
 *  |   toCollect: [tt1.x, tt2.x]
 *  |   outputs: [_fetchId(t1), _fetchId(t2)] -> [tt1.x, tt2.x]
 *  |
 *  | usedBeforeNextFetch: []
 * Join
 *  |   toCollect: [tt1.x, tt2.x]
 *  |   outputs:   lhs.outputs + rhs.outputs
 *  |
 *  | usedBeforeNextFetch: [tt1.id] -> [t1.id]
 *  +--Left
 *  |   |
 *  |  FetchOrEval
 *  |   |   toCollect: [t1.x, t1.id]                  unused: [t1.x]
 *  |   |   outputs:   [_fetchId(t1), t1.id]          # Rule: If there are any unused columns try to propagate fetch
 *  |   |
 *  |   | usedBeforeNextFetch: [t1.id]
 *  |  Collect:
 *  |       toCollect: [t1.x, t1.id]
 *  |       outputs:   [_fetchId(t1), t1.id]
 *  |
 *  | usedBeforeNextFetch: [tt2.id] -> [t2.id]
 *  +--Right
 *      ...
 * </pre>
 */
class FetchOrEval implements LogicalPlan {

    final LogicalPlan source;
    final List<Symbol> outputs;

    private final FetchMode fetchMode;
    private final boolean isLastFetch;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder,
                                      List<Symbol> outputs,
                                      FetchMode fetchMode,
                                      boolean isLastFetch) {
        return usedBeforeNextFetch -> {
            final LogicalPlan source;
            if (fetchMode == FetchMode.NEVER || !isLastFetch) {
                source = sourceBuilder.build(usedBeforeNextFetch);
            } else {
                source = sourceBuilder.build(Collections.emptySet());
            }
            if (source.outputs().equals(outputs)) {
                return source;
            }
            return new FetchOrEval(source, outputs, fetchMode, isLastFetch);
        };
    }

    private FetchOrEval(LogicalPlan source,
                        List<Symbol> outputs,
                        FetchMode fetchMode,
                        boolean isLastFetch) {
        this.source = source;
        this.fetchMode = fetchMode;
        this.isLastFetch = isLastFetch;
        if (isLastFetch) {
            this.outputs = outputs;
        } else {
            if (Symbols.containsColumn(source.outputs(), DocSysColumns.FETCHID)) {
                this.outputs = source.outputs();
            } else {
                this.outputs = outputs;
            }
        }
    }

    @Override
    public Plan build(Planner.Context plannerContext,
                      ProjectionBuilder projectionBuilder,
                      int limit,
                      int offset,
                      @Nullable OrderBy order,
                      @Nullable Integer pageSizeHint) {

        Plan plan = source.build(plannerContext, projectionBuilder, limit, offset, null, pageSizeHint);
        List<Symbol> sourceOutputs = source.outputs();
        if (sourceOutputs.equals(outputs)) {
            return plan;
        }

        if (Symbols.containsColumn(sourceOutputs, DocSysColumns.FETCHID)) {
            return planWithFetch(plannerContext, plan, sourceOutputs);
        }
        return planWithEvalProjection(plannerContext, plan, sourceOutputs);
    }

    private Plan planWithFetch(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        plan = Merge.ensureOnHandler(plan, plannerContext);
        Map<TableIdent, FetchSource> fetchSourceByTableId = new HashMap<>();
        LinkedHashSet<Reference> allFetchRefs = new LinkedHashSet<>();
        List<Symbol> expandedOutputs = expandOutputs(outputs, sourceOutputs);
        List<Symbol> expandedSourceOutputs = expandOutputs(sourceOutputs, Collections.emptyList());

        // TODO: extract function
        for (Map.Entry<DocTableRelation, List<Reference>> tableAndRefs : source.fetchReferencesByTable().entrySet()) {
            DocTableRelation tableRelation = tableAndRefs.getKey();
            DocTableInfo docTableInfo = tableRelation.tableInfo();
            List<Reference> fetchRefs = tableAndRefs.getValue();
            allFetchRefs.addAll(fetchRefs);
            ArrayList<InputColumn> fetchIdInputColumns = new ArrayList<>();
            int idx = 0;
            for (Symbol expandedSourceOutput : expandedSourceOutputs) {
                if (expandedSourceOutput instanceof Reference &&
                    ((Reference) expandedSourceOutput).ident().tableIdent().equals(docTableInfo.ident()) &&
                    ((Reference) expandedSourceOutput).ident().columnIdent().equals(DocSysColumns.FETCHID)) {

                    fetchIdInputColumns.add(new InputColumn(idx, expandedSourceOutput.valueType()));
                }
                idx++;
            }
            // TODO: in case of self-join get existing fetchRefs and concat unique
            fetchSourceByTableId.put(
                docTableInfo.ident(),
                new FetchSource(
                    docTableInfo.partitionedByColumns(),
                    fetchIdInputColumns,
                    fetchRefs
                )
            );
        }
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            allFetchRefs
        );
        // TODO: extract function, add explaination and try to simplify
        List<Symbol> fetchOutputs = new ArrayList<>(outputs.size());
        int idx = 0;
        for (Symbol output : outputs) {
            int sourceIdx = sourceOutputs.indexOf(output);
            if (sourceIdx > -1) {
                fetchOutputs.add(new InputColumn(sourceIdx, sourceOutputs.get(sourceIdx).valueType()));
            } else {
                Symbol expandedOutput = expandedOutputs.get(idx);
                Symbol fetchOutput = RefReplacer.replaceRefs(expandedOutput, r -> {
                    int expandedSourceIdx = expandedSourceOutputs.indexOf(r);
                    InputColumn fetchIdInputCol = new InputColumn(idxOfFetch(output, sourceOutputs), DataTypes.LONG);
                    if (expandedSourceIdx == -1) {
                        final Reference ref = r.granularity() == RowGranularity.DOC
                            ? DocReferences.toSourceLookup(r)
                            : r;
                        return new FetchReference(fetchIdInputCol, ref);
                    }
                    return new InputColumn(expandedSourceIdx, expandedSourceOutputs.get(expandedSourceIdx).valueType());
                });
                fetchOutput = FieldReplacer.replaceFields(fetchOutput, f -> {
                    int index = sourceOutputs.indexOf(f);
                    if (index == -1) {
                        // otherwise the Field should've been expanded to a Reference
                        // only fields which are available in the sourceOutputs are not expanded
                        throw new IllegalArgumentException("Field " + f + " must be present in " + sourceOutputs);
                    }
                    Symbol source = sourceOutputs.get(index);
                    return new InputColumn(index, source.valueType());
                });
                fetchOutputs.add(fetchOutput);
            }
            idx++;
        }
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            fetchSourceByTableId,
            fetchOutputs,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        plan.addProjection(fetchProjection);
        return new QueryThenFetch(plan, fetchPhase);
    }

    private int idxOfFetch(Symbol output, List<Symbol> sourceOutputs) {
        // TODO: explain what's happening here or figure out a way to make this simpler
        final int[] idx = new int[] { 0 };
        Map<Symbol, Symbol> expressionMapping = source.expressionMapping();
        Symbol resolvedToQueriedTable = FieldReplacer.replaceFields(output, f -> {
            if (f.relation() instanceof QueriedTableRelation) {
                return f;
            }
            Symbol oldMapped = f;
            Symbol newMapped = expressionMapping.get(oldMapped);
            while (newMapped != null) {
                final boolean[] isResolvedToQueriedTable = new boolean[] { false };
                FieldsVisitor.visitFields(newMapped, f1 -> {
                    isResolvedToQueriedTable[0] = isResolvedToQueriedTable[0] || f1.relation() instanceof QueriedTableRelation;
                });
                if (isResolvedToQueriedTable[0]) {
                    return newMapped;
                }
                oldMapped = newMapped;
                newMapped = expressionMapping.get(oldMapped);
            }
            return oldMapped;
        });
        FieldsVisitor.visitFields(resolvedToQueriedTable, f -> {
            int i = 0;
            for (Symbol sourceOutput : sourceOutputs) {
                if (sourceOutput instanceof Field &&
                    ((Field) sourceOutput).relation().equals(f.relation()) &&
                    ((Field) sourceOutput).path().outputName().equals(DocSysColumns.FETCHID.outputName())) {
                    idx[0] = i;
                    break;
                }
                i++;
            }
        });
        return idx[0];
    }

    /**
     * Expand/resolve the expressions across relation boundary.
     *
     * This is necessary if a fetch-propagation occurred.
     *
     * Example:
     * <pre>
     *     select xx + xx from (select x + x as xx from t) tt
     *
     *     outputs: [tt.xx + tt.xx]
     *     sourceOutputs: [x + x]
     *
     *     result: (x + x) + (x + x)
     *     (x = Reference)
     * </pre>
     *
     * @return A list of symbols with the same number of items as {@code toExpand}
     *         All fields will be replaced with References, or a Function containing References.
     *         The exception are fields which are present in {@code sourceOutputs}, these are kept as is.
     */
    private List<Symbol> expandOutputs(List<Symbol> toExpand, List<Symbol> sourceOutputs) {
        Function<Symbol, Symbol> mapper = LogicalPlan.getMapper(source.expressionMapping());
        return Lists2.copyAndReplace(toExpand, FieldReplacer.bind(s -> {
            int idx = sourceOutputs.indexOf(s);
            if (idx > -1) {
                return sourceOutputs.get(idx);
            }

            Symbol s1 = s;
            Symbol s2 = mapper.apply(s1);
            while (s2 != s1) {
                s1 = s2;
                s2 = mapper.apply(s1);
            }
            return s2;
        }));
    }

    private Plan planWithEvalProjection(Planner.Context plannerContext, Plan plan, List<Symbol> sourceOutputs) {
        if (plan.resultDescription().orderBy() != null) {
            plan = Merge.ensureOnHandler(plan, plannerContext);
        }
        InputColumns.Context ctx = new InputColumns.Context(sourceOutputs);
        plan.addProjection(new EvalProjection(InputColumns.create(this.outputs, ctx)));
        return plan;
    }

    @Override
    public LogicalPlan tryCollapse() {
        LogicalPlan collapsed = source.tryCollapse();
        if (collapsed == this) {
            return this;
        }
        return new FetchOrEval(collapsed, outputs, fetchMode, isLastFetch);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return source.expressionMapping();
    }

    @Override
    public Map<DocTableRelation, List<Reference>> fetchReferencesByTable() {
        return source.fetchReferencesByTable();
    }

    @Override
    public String toString() {
        return "FetchOrEval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }
}
