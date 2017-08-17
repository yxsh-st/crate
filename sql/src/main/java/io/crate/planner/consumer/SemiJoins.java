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

package io.crate.planner.consumer;

import io.crate.analyze.QuerySpec;
import io.crate.analyze.TwoTableJoin;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.FuncSymbols;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.table.Operation;
import io.crate.operation.operator.Operator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.crate.analyze.expressions.ExpressionAnalyzer.castIfNeededOrFail;
import static io.crate.operation.operator.Operators.LOGICAL_OPERATORS;

public class SemiJoins {

    /**
     * Try to rewrite a QueriedRelation into a SemiJoin:
     *
     * <pre>
     *
     *     select x from t1 where x in (select id from t2)
     *               |
     *               v
     *     select t1.x from t1 SEMI JOIN (select id from t2) t2 on t1.x = t2.id
     * </pre>
     *
     * (Note that it's not possible to write a SemiJoin directly using SQL. But the example above is semantically close.)
     *
     * This rewrite isn't always possible - certain conditions need to be met for the semi-join to have the same
     * semantics as the subquery.
     *
     * @return the rewritten relation or null if a rewrite wasn't possible.
     */
    @Nullable
    public static QueriedRelation tryRewrite(QueriedRelation rel) {
        WhereClause where = rel.querySpec().where();
        if (!where.hasQuery()) {
            return null;
        }
        List<Function> rewriteCandidates = gatherRewriteCandidates(where.query());
        if (rewriteCandidates.isEmpty()) {
            return null;
        }
        if (rewriteCandidates.size() > 1) {
            return null;  // TODO: support this as well
        }
        Function rewriteCandidate = rewriteCandidates.get(0);
        SelectSymbol selectSymbol = getSubquery(rewriteCandidate.arguments().get(1));
        assert selectSymbol != null : "rewriteCandidate must contain a selectSymbol";

        removeRewriteCandidatesFromWhere(rel, rewriteCandidate);

        // Turn Ref(x) back into Field(rel, x); it's required to for TwoTableJoin structure;
        // (QuerySplitting logic that follows in the Planner is based on Fields)
        java.util.function.Function<Symbol, Symbol> refsToFields = st -> RefReplacer.replaceRefs(st,
            r -> rel.getField(r.ident().columnIdent(), Operation.READ));
        QuerySpec newQS = rel.querySpec().copyAndReplace(refsToFields);
        Symbol joinCondition;

        try {
            joinCondition = makeJoinCondition(
                (Function) RefReplacer.replaceRefs(rewriteCandidate, r -> rel.getField(r.ident().columnIdent(), Operation.READ)),
                selectSymbol.relation());
        } catch (Exception e) {
            /* TODO: Remove this limitation

             * rel.getField(..) doesn't work in a query like this:
             *      select count(*) from t2 where t2.id in (select id from t1)
             * Because t2.id is not in the outputs of (select count(*) from t2)
             * This causes replaceRefs here to fail.
             */
            return null;
        }
        TwoTableJoin twoTableJoin = new TwoTableJoin(
            newQS,
            rel,
            selectSymbol.relation(),
            Optional.empty(),
            JoinPair.of(
                rel.getQualifiedName(),
                selectSymbol.relation().getQualifiedName(),
                JoinType.SEMI,
                joinCondition
            )
        );
        return twoTableJoin;
    }

    private static void removeRewriteCandidatesFromWhere(QueriedRelation rel, Function rewriteCandidate) {
        rel.querySpec().where().replace(st -> FuncSymbols.mapNodes(st, f -> {
            if (f == rewriteCandidate) {
                return Literal.BOOLEAN_TRUE;
            }
            return f;
        }));
    }

    // TODO: could consider using a custom class structure instead of Function ?
    static List<Function> gatherRewriteCandidates(Symbol query) {
        ArrayList<Function> candidates = new ArrayList<>();
        RewriteCandidateGatherer.INSTANCE.process(query, candidates);
        return candidates;
    }

    @Nullable
    private static SelectSymbol getSubquery(Symbol symbol) {
        // TODO: need to properly unwrap casts
        // and maybe not add them in the first place if unnecessary
        if (symbol instanceof Function && ((Function) symbol).info().ident().name().startsWith("to_")) {
            if (((Function) symbol).arguments().get(0) instanceof SelectSymbol) {
                return ((SelectSymbol) ((Function) symbol).arguments().get(0));
            }
            return null;
        }
        if (symbol instanceof SelectSymbol) {
            return ((SelectSymbol) symbol);
        }
        return null;
    }

    /**
     * t1.x IN (select y from t2)  --> SEMI JOIN t1 on t1.x = t2.y
     */
    private static Symbol makeJoinCondition(Function rewriteCandidate, QueriedRelation subQuery) {
        String name = rewriteCandidate.info().ident().name();
        assert name.startsWith(AnyOperator.OPERATOR_PREFIX) : "Can only create a join condition from any_";

        List<Symbol> args = rewriteCandidate.arguments();
        Symbol firstArg = args.get(0);
        List<DataType> newArgTypes = Arrays.asList(firstArg.valueType(), firstArg.valueType());

        FunctionIdent joinCondIdent = new FunctionIdent(
            Operator.PREFIX + name.substring(AnyOperator.OPERATOR_PREFIX.length()), newArgTypes);
        return new Function(
            new FunctionInfo(joinCondIdent, DataTypes.BOOLEAN),
            Arrays.asList(firstArg, castIfNeededOrFail(subQuery.fields().get(0), firstArg.valueType()))
        );
    }

    private static boolean isSuitableSubqueryForSemiJoin(QueriedRelation relation) {
        return true;

        // QuerySpec querySpec = relation.querySpec();
        // // Would also be unsuitable if it's a union, but we don't support UNION yet.
        // return !querySpec.hasAggregates()
        //     && !querySpec.groupBy().isPresent()
        //     && !querySpec.having().isPresent()
        //     && !querySpec.orderBy().isPresent()
        //     && !querySpec.limit().isPresent()
        //     && !querySpec.offset().isPresent();
    }


    private static class RewriteCandidateGatherer extends SymbolVisitor<List<Function>, Boolean> {

        static final RewriteCandidateGatherer INSTANCE = new RewriteCandidateGatherer();

        @Override
        protected Boolean visitSymbol(Symbol symbol, List<Function> context) {
            return true;
        }

        @Override
        public Boolean visitFunction(Function func, List<Function> candidates) {
            String funcName = func.info().ident().name();

            /* Cannot rewrite a `op ANY subquery` expression into a semi-join if it's beneath a OR because
             * `op ANY subquery` has different semantics in case of NULL values than a semi-join would have
             */
            if (funcName.equals(OrOperator.NAME)) {
                candidates.clear();
                return false;
            }

            if (LOGICAL_OPERATORS.contains(funcName)) {
                for (Symbol arg : func.arguments()) {
                    Boolean continueTraversal = process(arg, candidates);
                    if (!continueTraversal) {
                        return false;
                    }
                }
                return true;
            }

            if (funcName.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                maybeAddSubQueryAsCandidate(func, candidates);
            }
            return true;
        }

        private static void maybeAddSubQueryAsCandidate(Function func, List<Function> candidates) {
            SelectSymbol subQuery = getSubquery(func.arguments().get(1));
            if (subQuery == null) {
                return;
            }
            if (subQuery.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES
                && isSuitableSubqueryForSemiJoin(subQuery.relation())) {

                candidates.add(func);
            }
        }
    }
}
