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

import java.util.HashMap;
import java.util.List;

import com.wplatform.ddal.command.Parser;
import com.wplatform.ddal.command.dml.Select;
import com.wplatform.ddal.command.dml.SelectListColumnResolver;
import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.schema.Constant;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueBoolean;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private final Database database;
    private final String schemaName;
    private final String tableAlias;
    private final String columnName;
    private ColumnResolver columnResolver;
    private int queryLevel;
    private Column column;
    private boolean evaluatable;

    public ExpressionColumn(Database database, Column column) {
        this.database = database;
        this.column = column;
        this.schemaName = null;
        this.tableAlias = null;
        this.columnName = null;
    }

    public ExpressionColumn(Database database, String schemaName,
                            String tableAlias, String columnName) {
        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    @Override
    public String getSQL() {
        String sql;
        boolean quote = database.getSettings().databaseToUpper;
        if (column != null) {
            sql = column.getSQL();
        } else {
            sql = quote ? Parser.quoteIdentifier(columnName) : columnName;
        }
        if (tableAlias != null) {
            String a = quote ? Parser.quoteIdentifier(tableAlias) : tableAlias;
            sql = a + "." + sql;
        }
        if (schemaName != null) {
            String s = quote ? Parser.quoteIdentifier(schemaName) : schemaName;
            sql = s + "." + sql;
        }
        return sql;
    }

    public TableFilter getTableFilter() {
        return columnResolver == null ? null : columnResolver.getTableFilter();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (tableAlias != null && !database.equalsIdentifiers(
                tableAlias, resolver.getTableAlias())) {
            return;
        }
        if (schemaName != null && !database.equalsIdentifiers(
                schemaName, resolver.getSchemaName())) {
            return;
        }
        for (Column col : resolver.getColumns()) {
            String n = col.getName();
            if (database.equalsIdentifiers(columnName, n)) {
                mapColumn(resolver, col, level);
                return;
            }
        }
        if (database.equalsIdentifiers(Column.ROWID, columnName)) {
            Column col = resolver.getRowIdColumn();
            if (col != null) {
                mapColumn(resolver, col, level);
                return;
            }
        }
        Column[] columns = resolver.getSystemColumns();
        for (int i = 0; columns != null && i < columns.length; i++) {
            Column col = columns[i];
            if (database.equalsIdentifiers(columnName, col.getName())) {
                mapColumn(resolver, col, level);
                return;
            }
        }
    }

    private void mapColumn(ColumnResolver resolver, Column col, int level) {
        if (this.columnResolver == null) {
            queryLevel = level;
            column = col;
            this.columnResolver = resolver;
        } else if (queryLevel == level && this.columnResolver != resolver) {
            if (resolver instanceof SelectListColumnResolver) {
                // ignore - already mapped, that's ok
            } else {
                throw DbException.get(ErrorCode.AMBIGUOUS_COLUMN_NAME_1, columnName);
            }
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (columnResolver == null) {
            Schema schema = session.getDatabase().findSchema(
                    tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
            if (schema != null) {
                Constant constant = schema.findConstant(columnName);
                if (constant != null) {
                    return constant.getValue();
                }
            }
            String name = columnName;
            if (tableAlias != null) {
                name = tableAlias + "." + name;
                if (schemaName != null) {
                    name = schemaName + "." + name;
                }
            }
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, name);
        }
        return columnResolver.optimize(this, column);
    }

    @Override
    public void updateAggregate(Session session) {
        Value now = columnResolver.getValue(column);
        Select select = columnResolver.getSelect();
        if (select == null) {
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        HashMap<Expression, Object> values = select.getCurrentGroup();
        if (values == null) {
            // this is a different level (the enclosing query)
            return;
        }
        Value v = (Value) values.get(this);
        if (v == null) {
            values.put(this, now);
        } else {
            if (!database.areEqual(now, v)) {
                throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
            }
        }
    }

    @Override
    public Value getValue(Session session) {
        Select select = columnResolver.getSelect();
        if (select != null) {
            HashMap<Expression, Object> values = select.getCurrentGroup();
            if (values != null) {
                Value v = (Value) values.get(this);
                if (v != null) {
                    return v;
                }
            }
        }
        Value value = columnResolver.getValue(column);
        if (value == null) {
            columnResolver.getValue(column);
            throw DbException.get(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return value;
    }

    @Override
    public int getType() {
        return column.getType();
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (columnResolver != null && tableFilter == columnResolver.getTableFilter()) {
            evaluatable = b;
        }
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public int getScale() {
        return column.getScale();
    }

    @Override
    public long getPrecision() {
        return column.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return column.getDisplaySize();
    }

    public String getOriginalColumnName() {
        return columnName;
    }

    public String getOriginalTableAliasName() {
        return tableAlias;
    }

    @Override
    public String getColumnName() {
        return columnName != null ? columnName : column.getName();
    }

    @Override
    public String getSchemaName() {
        Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    }

    @Override
    public String getTableName() {
        Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    @Override
    public String getAlias() {
        return column == null ? columnName : column.getName();
    }

    @Override
    public boolean isAutoIncrement() {
        return column.getSequence() != null;
    }

    @Override
    public int getNullable() {
        return column.isNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
            case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
                return false;
            case ExpressionVisitor.READONLY:
            case ExpressionVisitor.DETERMINISTIC:
            case ExpressionVisitor.QUERY_COMPARABLE:
                return true;
            case ExpressionVisitor.INDEPENDENT:
                return this.queryLevel < visitor.getQueryLevel();
            case ExpressionVisitor.EVALUATABLE:
                // if the current value is known (evaluatable set)
                // or if this columns belongs to a 'higher level' query and is
                // therefore just a parameter
                if (database.getSettings().nestedJoins) {
                    if (visitor.getQueryLevel() < this.queryLevel) {
                        return true;
                    }
                    if (getTableFilter() == null) {
                        return false;
                    }
                    return getTableFilter().isEvaluatable();
                }
                return evaluatable || visitor.getQueryLevel() < this.queryLevel;
            case ExpressionVisitor.NOT_FROM_RESOLVER:
                return columnResolver != visitor.getResolver();
            case ExpressionVisitor.GET_DEPENDENCIES:
                if (column != null) {
                    visitor.addDependency(column.getTable());
                }
                return true;
            case ExpressionVisitor.GET_COLUMNS:
                visitor.addColumn(column);
                return true;
            default:
                throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 2;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        TableFilter tf = getTableFilter();
        if (filter == tf && column.getType() == Value.BOOLEAN) {
            IndexCondition cond = IndexCondition.get(
                    Comparison.EQUAL, this, ValueExpression.get(
                            ValueBoolean.get(true)));
            filter.addIndexCondition(cond);
        }
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.get(false)));
    }

    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        if (getTableFilter() == filter) {
            return getSQL();
        }
        Value value = this.getValue(filter.getSession());
        container.add(value);
        return "?";
    }

}
