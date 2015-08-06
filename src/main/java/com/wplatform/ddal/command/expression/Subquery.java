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

import java.util.ArrayList;

import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueArray;
import com.wplatform.ddal.value.ValueNull;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public class Subquery extends Expression {

    private final Query query;
    private Expression expression;

    public Subquery(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        ResultInterface result = query.query(2);
        try {
            int rowcount = result.getRowCount();
            if (rowcount > 1) {
                throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
            }
            Value v;
            if (rowcount <= 0) {
                v = ValueNull.INSTANCE;
            } else {
                result.next();
                Value[] values = result.currentRow();
                if (result.getVisibleColumnCount() == 1) {
                    v = values[0];
                } else {
                    v = ValueArray.get(values);
                }
            }
            return v;
        } finally {
            result.close();
        }
    }

    @Override
    public int getType() {
        return getExpression().getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(Session session) {
        query.prepare();
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public int getScale() {
        return getExpression().getScale();
    }

    @Override
    public long getPrecision() {
        return getExpression().getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return getExpression().getDisplaySize();
    }

    @Override
    public String getSQL() {
        return "(" + query.getPlanSQL() + ")";
    }

    @Override
    public void updateAggregate(Session session) {
        query.updateAggregate(session);
    }

    private Expression getExpression() {
        if (expression == null) {
            ArrayList<Expression> expressions = query.getExpressions();
            int columnCount = query.getColumnCount();
            if (columnCount == 1) {
                expression = expressions.get(0);
            } else {
                Expression[] list = new Expression[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    list[i] = expressions.get(i);
                }
                expression = new ExpressionList(list);
            }
        }
        return expression;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpression().getExpressionColumns(session);
    }
}
