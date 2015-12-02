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

import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueInt;

/**
 * Represents the ROWNUM function.
 */
public class Rownum extends Expression {

    private final Prepared prepared;

    public Rownum(Prepared prepared) {
        this.prepared = prepared;
    }

    @Override
    public Value getValue(Session session) {
        return ValueInt.get(prepared.getCurrentRowNumber());
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    @Override
    public String getSQL() {
        return "ROWNUM()";
    }

    @Override
    public void updateAggregate(Session session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
            case ExpressionVisitor.QUERY_COMPARABLE:
            case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            case ExpressionVisitor.DETERMINISTIC:
            case ExpressionVisitor.INDEPENDENT:
                return false;
            case ExpressionVisitor.EVALUATABLE:
            case ExpressionVisitor.READONLY:
            case ExpressionVisitor.NOT_FROM_RESOLVER:
            case ExpressionVisitor.GET_DEPENDENCIES:
            case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            case ExpressionVisitor.GET_COLUMNS:
                // if everything else is the same, the rownum is the same
                return true;
            default:
                throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

}
