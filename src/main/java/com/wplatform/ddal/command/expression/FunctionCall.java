/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplatform.ddal.command.expression;

import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.value.ValueResultSet;

/**
 * This interface is used by the built-in functions,
 * as well as the user-defined functions.
 */
public interface FunctionCall {

    /**
     * Get the name of the function.
     *
     * @return the name
     */
    String getName();

    /**
     * Get an empty result set with the column names set.
     *
     * @param session  the session
     * @param nullArgs the argument list (some arguments may be null)
     * @return the empty result set
     */
    ValueResultSet getValueForColumnList(Session session, Expression[] nullArgs);

    /**
     * Get the data type.
     *
     * @return the data type
     */
    int getType();

    /**
     * Optimize the function if possible.
     *
     * @param session the session
     * @return the optimized expression
     */
    Expression optimize(Session session);

    /**
     * Get the function arguments.
     *
     * @return argument list
     */
    Expression[] getArgs();

    /**
     * Get the SQL snippet of the function (including arguments).
     *
     * @return the SQL snippet.
     */
    String getSQL();

    /**
     * Whether the function always returns the same result for the same
     * parameters.
     *
     * @return true if it does
     */
    boolean isDeterministic();

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     *
     * @return true if it should be.
     */
    boolean isBufferResultSetToLocalTemp();

}
