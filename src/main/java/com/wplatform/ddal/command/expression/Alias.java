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
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.value.Value;

/**
 * A column alias as in SELECT 'Hello' AS NAME ...
 */
public class Alias extends Expression {

    private final String alias;
    private final boolean aliasColumnName;
    private Expression expr;

    public Alias(Expression expression, String alias, boolean aliasColumnName) {
        this.expr = expression;
        this.alias = alias;
        this.aliasColumnName = aliasColumnName;
    }

    @Override
    public Expression getNonAliasExpression() {
        return expr;
    }

    @Override
    public Value getValue(Session session) {
        return expr.getValue(session);
    }

    @Override
    public int getType() {
        return expr.getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        expr.mapColumns(resolver, level);
    }

    @Override
    public Expression optimize(Session session) {
        expr = expr.optimize(session);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    @Override
    public int getScale() {
        return expr.getScale();
    }

    @Override
    public long getPrecision() {
        return expr.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return expr.getDisplaySize();
    }

    @Override
    public boolean isAutoIncrement() {
        return expr.isAutoIncrement();
    }

    @Override
    public String getSQL() {
        return expr.getSQL() + " AS " + Parser.quoteIdentifier(alias);
    }

    @Override
    public void updateAggregate(Session session) {
        expr.updateAggregate(session);
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public int getNullable() {
        return expr.getNullable();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return expr.getCost();
    }

    @Override
    public String getTableName() {
        if (aliasColumnName) {
            return super.getTableName();
        }
        return expr.getTableName();
    }

    @Override
    public String getColumnName() {
        if (!(expr instanceof ExpressionColumn) || aliasColumnName) {
            return super.getColumnName();
        }
        return expr.getColumnName();
    }


    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        return expr.exportParameters(filter, container) + " AS " + Parser.quoteIdentifier(alias);
    }

}
