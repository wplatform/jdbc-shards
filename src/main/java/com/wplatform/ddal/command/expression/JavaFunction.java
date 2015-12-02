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

import com.wplatform.ddal.command.Parser;
import com.wplatform.ddal.dbobject.FunctionAlias;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.*;

/**
 * This class wraps a user-defined function.
 */
public class JavaFunction extends Expression implements FunctionCall {

    private final FunctionAlias functionAlias;
    private final FunctionAlias.JavaMethod javaMethod;
    private final Expression[] args;

    public JavaFunction(FunctionAlias functionAlias, Expression[] args) {
        this.functionAlias = functionAlias;
        this.javaMethod = functionAlias.findJavaMethod(args);
        this.args = args;
    }

    @Override
    public Value getValue(Session session) {
        return javaMethod.getValue(session, args, false);
    }

    @Override
    public int getType() {
        return javaMethod.getDataType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : args) {
            e.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        boolean allConst = isDeterministic();
        for (int i = 0, len = args.length; i < len; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            allConst &= e.isConstant();
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            if (e != null) {
                e.setEvaluatable(tableFilter, b);
            }
        }
    }

    @Override
    public int getScale() {
        return DataType.getDataType(getType()).defaultScale;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        // TODO always append the schema once FUNCTIONS_IN_SCHEMA is enabled
        if (functionAlias.getDatabase().getSettings().functionsInSchema ||
                !functionAlias.getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            buff.append(
                    Parser.quoteIdentifier(functionAlias.getSchema().getName()))
                    .append('.');
        }
        buff.append(Parser.quoteIdentifier(functionAlias.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public void updateAggregate(Session session) {
        for (Expression e : args) {
            if (e != null) {
                e.updateAggregate(session);
            }
        }
    }

    @Override
    public String getName() {
        return functionAlias.getName();
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
                                                Expression[] argList) {
        Value v = javaMethod.getValue(session, argList, true);
        return v == ValueNull.INSTANCE ? null : (ValueResultSet) v;
    }

    @Override
    public Expression[] getArgs() {
        return args;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
            case ExpressionVisitor.DETERMINISTIC:
                if (!isDeterministic()) {
                    return false;
                }
                // only if all parameters are deterministic as well
                break;
            case ExpressionVisitor.GET_DEPENDENCIES:
                visitor.addDependency(functionAlias);
                break;
            default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = javaMethod.hasConnectionParam() ? 25 : 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public boolean isDeterministic() {
        return functionAlias.isDeterministic();
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        switch (getType()) {
            case Value.RESULT_SET:
                ValueResultSet rs = getValueForColumnList(session, getArgs());
                return getExpressionColumns(session, rs.getResultSet());
            case Value.ARRAY:
                return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }

    @Override
    public boolean isBufferResultSetToLocalTemp() {
        return functionAlias.isBufferResultSetToLocalTemp();
    }


    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        StatementBuilder buff = new StatementBuilder();
        // TODO always append the schema once FUNCTIONS_IN_SCHEMA is enabled
        if (functionAlias.getDatabase().getSettings().functionsInSchema ||
                !functionAlias.getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            buff.append(
                    Parser.quoteIdentifier(functionAlias.getSchema().getName()))
                    .append('.');
        }
        buff.append(Parser.quoteIdentifier(functionAlias.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.exportParameters(filter, container));
        }
        return buff.append(')').toString();
    }

}
