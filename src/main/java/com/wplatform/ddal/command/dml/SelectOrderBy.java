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
package com.wplatform.ddal.command.dml;

import com.wplatform.ddal.command.expression.Expression;

/**
 * Describes one element of the ORDER BY clause of a query.
 */
public class SelectOrderBy {

    /**
     * The order by expression.
     */
    public Expression expression;

    /**
     * The column index expression. This can be a column index number (1 meaning
     * the first column of the select list) or a parameter (the parameter is a
     * number representing the column index number).
     */
    public Expression columnIndexExpr;

    /**
     * If the column should be sorted descending.
     */
    public boolean descending;

    /**
     * If NULL should be appear first.
     */
    public boolean nullsFirst;

    /**
     * If NULL should be appear at the end.
     */
    public boolean nullsLast;

    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        if (expression != null) {
            buff.append('=').append(expression.getSQL());
        } else {
            buff.append(columnIndexExpr.getSQL());
        }
        if (descending) {
            buff.append(" DESC");
        }
        if (nullsFirst) {
            buff.append(" NULLS FIRST");
        } else if (nullsLast) {
            buff.append(" NULLS LAST");
        }
        return buff.toString();
    }

}
