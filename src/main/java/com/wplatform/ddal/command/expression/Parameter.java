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

import java.util.List;

import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueBoolean;
import com.wplatform.ddal.value.ValueNull;

/**
 * A parameter of a prepared statement.
 */
public class Parameter extends Expression implements ParameterInterface {

    private final int index;
    private Value value;
    private Column column;

    public Parameter(int index) {
        this.index = index;
    }

    @Override
    public String getSQL() {
        return "?" + (index + 1);
    }

    @Override
    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway
        // removed
        this.value = v;
    }

    public void setValue(Value v) {
        this.value = v;
    }

    @Override
    public Value getParamValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    @Override
    public Value getValue(Session session) {
        return getParamValue();
    }

    @Override
    public int getType() {
        if (value != null) {
            return value.getType();
        }
        if (column != null) {
            return column.getType();
        }
        return Value.UNKNOWN;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // can't map
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // not bound
    }

    @Override
    public int getScale() {
        if (value != null) {
            return value.getScale();
        }
        if (column != null) {
            return column.getScale();
        }
        return 0;
    }

    @Override
    public long getPrecision() {
        if (value != null) {
            return value.getPrecision();
        }
        if (column != null) {
            return column.getPrecision();
        }
        return 0;
    }

    @Override
    public int getDisplaySize() {
        if (value != null) {
            return value.getDisplaySize();
        }
        if (column != null) {
            return column.getDisplaySize();
        }
        return 0;
    }

    @Override
    public void updateAggregate(Session session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
            case ExpressionVisitor.EVALUATABLE:
                // the parameter _will_be_ evaluatable at execute time
            case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
                // it is checked independently if the value is the same as the last
                // time
            case ExpressionVisitor.NOT_FROM_RESOLVER:
            case ExpressionVisitor.QUERY_COMPARABLE:
            case ExpressionVisitor.GET_DEPENDENCIES:
            case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
            case ExpressionVisitor.DETERMINISTIC:
            case ExpressionVisitor.READONLY:
            case ExpressionVisitor.GET_COLUMNS:
                return true;
            case ExpressionVisitor.INDEPENDENT:
                return value != null;
            default:
                throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.get(false)));
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        container.add(value);
        return "?";
    }

}
