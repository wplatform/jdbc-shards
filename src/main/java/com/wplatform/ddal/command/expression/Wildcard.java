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

import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.value.Value;

/**
 * A wildcard expression as in SELECT * FROM TEST.
 * This object is only used temporarily during the parsing phase, and later
 * replaced by column expressions.
 */
public class Wildcard extends Expression {
    private final String schema;
    private final String table;

    public Wildcard(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public boolean isWildcard() {
        return true;
    }

    @Override
    public Value getValue(Session session) {
        throw DbException.throwInternalError();
    }

    @Override
    public int getType() {
        throw DbException.throwInternalError();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public Expression optimize(Session session) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        DbException.throwInternalError();
    }

    @Override
    public int getScale() {
        throw DbException.throwInternalError();
    }

    @Override
    public long getPrecision() {
        throw DbException.throwInternalError();
    }

    @Override
    public int getDisplaySize() {
        throw DbException.throwInternalError();
    }

    @Override
    public String getTableAlias() {
        return table;
    }

    @Override
    public String getSchemaName() {
        return schema;
    }

    @Override
    public String getSQL() {
        if (table == null) {
            return "*";
        }
        return StringUtils.quoteIdentifier(table) + ".*";
    }

    @Override
    public void updateAggregate(Session session) {
        DbException.throwInternalError();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        throw DbException.throwInternalError();
    }

    @Override
    public int getCost() {
        throw DbException.throwInternalError();
    }

}
