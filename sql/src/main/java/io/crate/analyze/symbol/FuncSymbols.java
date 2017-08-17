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

package io.crate.analyze.symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class FuncSymbols {

    /**
     * Apply the mapper function on all nodes in a symbolTree.
     */
    public static Symbol mapNodes(Symbol st, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
        return Visitor.INSTANCE.process(st, mapper);
    }

    private static class Visitor extends SymbolVisitor<java.util.function.Function<? super Function, ? extends Symbol>, Symbol> {

        private static final Visitor INSTANCE = new Visitor();

        @Override
        protected Symbol visitSymbol(Symbol symbol, java.util.function.Function<? super Function, ? extends Symbol> context) {
            return symbol;
        }

        @Override
        public Symbol visitFunction(Function func, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
            switch (func.arguments().size()) {
                // specialized functions to avoid extra list allocations
                case 1:
                    return mapper.apply(oneArg(func, mapper));

                case 2:
                    return mapper.apply(twoArgs(func, mapper));

                default:
                    return mapper.apply(manyArgs(func, mapper));
            }
        }

        private Function manyArgs(Function func, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
            List<Symbol> args = func.arguments();
            List<Symbol> newArgs = new ArrayList<>();
            boolean changed = false;
            for (Symbol arg : args) {
                Symbol newArg = process(arg, mapper);

                changed = changed || arg != newArg;
                newArgs.add(newArg);
            }
            if (changed) {
                return new Function(func.info(), newArgs);
            }
            return func;
        }

        private Function twoArgs(Function func, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
            assert func.arguments().size() == 2 : "size of arguments must be two";

            Symbol arg1 = func.arguments().get(0);
            Symbol newArg1 = process(arg1, mapper);

            Symbol arg2 = func.arguments().get(1);
            Symbol newArg2 = process(arg2, mapper);

            if (arg1 == newArg1 && arg2 == newArg2) {
                return func;
            }
            return new Function(func.info(), Arrays.asList(newArg1, newArg2));
        }

        private Function oneArg(Function func, java.util.function.Function<? super Function, ? extends Symbol> mapper) {
            assert func.arguments().size() == 1 : "size of arguments must be one";

            Symbol arg = func.arguments().get(0);
            Symbol newArg = process(arg, mapper);
            if (arg == newArg) {
                return func;
            }
            return new Function(func.info(), Arrays.asList(newArg));
        }
    }
}
