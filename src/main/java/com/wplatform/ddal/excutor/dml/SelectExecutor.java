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
package com.wplatform.ddal.excutor.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.wplatform.ddal.command.dml.Select;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ExpressionColumn;
import com.wplatform.ddal.command.expression.Wildcard;
import com.wplatform.ddal.config.TableConfig;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.LocalResult;
import com.wplatform.ddal.result.ResultTarget;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.result.SortOrder;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SelectExecutor extends PreparedRoutingExecutor<Select> {

    /**
     * @param prepared
     */
    public SelectExecutor(Select prepared) {
        super(prepared);
    }


    @Override
    public LocalResult executeQuery(int maxRows, ResultTarget target) {
        Expression limitExpr = prepared.getLimit();
        ArrayList<Expression> expressions = prepared.getExpressions();
        SortOrder sort = prepared.getSort();
        boolean distinct = prepared.isDistinct();
        boolean isDistinctQuery = prepared.isDistinctQuery();
        boolean sortUsingIndex = prepared.isSortUsingIndex();
        boolean randomAccessResult = prepared.isRandomAccessResult();
        boolean isGroupQuery = prepared.isGroupQuery();
        boolean isGroupSortedQuery = prepared.isGroupSortedQuery();
        boolean isQuickAggregateQuery = prepared.isQuickAggregateQuery();
        Expression offsetExpr = prepared.getOffset();
        TableFilter topTableFilter = prepared.getTopTableFilter();
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        int columnCount = expressions.size();
        LocalResult result = null;
        if (target == null ||
                !session.getDatabase().getSettings().optimizeInsertFromSelect) {
            result = createLocalResult(result);
        }
        if (sort != null && (!sortUsingIndex || distinct)) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
        }
        if (distinct && !isDistinctQuery) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result = createLocalResult(result);
        }
        if (isGroupQuery && !isGroupSortedQuery) {
            result = createLocalResult(result);
        }
        if (limitRows >= 0 || offsetExpr != null) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        boolean exclusive = isForUpdate && !isForUpdateMvcc;
        if (isForUpdateMvcc) {
            if (isGroupQuery) {
                throw DbException.getUnsupportedException(
                        "FOR UPDATE && GROUP");
            } else if (distinct) {
                throw DbException.getUnsupportedException(
                        "FOR UPDATE && DISTINCT");
            } else if (isQuickAggregateQuery) {
                throw DbException.getUnsupportedException(
                        "FOR UPDATE && AGGREGATE");
            } else if (topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException(
                        "FOR UPDATE && JOIN");
            }
        }
        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if (isQuickAggregateQuery) {
                queryQuick(columnCount, to);
            } else if (isGroupQuery) {
                if (isGroupSortedQuery) {
                    queryGroupSorted(columnCount, to);
                } else {
                    queryGroup(columnCount, result);
                }
            } else if (isDistinctQuery) {
                queryDistinct(to, limitRows);
            } else {
                queryFlat(columnCount, to, limitRows);
            }
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        if (limitRows >= 0) {
            result.setLimit(limitRows);
        }
        if (result != null) {
            result.done();
            if (target != null) {
                while (result.next()) {
                    target.addRow(result.currentRow());
                }
                result.close();
                return null;
            }
            return result;
        }
        return null;
    }


    private LocalResult createLocalResult(LocalResult old) {
        Expression[] expressionArray = prepared.getExpressionArray();
        int visibleColumnCount = prepared.getVisibleColumnCount();
        return old != null ? old : new LocalResult(session, expressionArray,
                visibleColumnCount);
    }

    private void expandColumnList() {
        Database db = session.getDatabase();
        ArrayList<Expression> expressions = prepared.getExpressions();
        ArrayList<TableFilter> filters = prepared.getFilters();
        // the expressions may change within the loop
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                int temp = i;
                expressions.remove(i);
                for (TableFilter filter : filters) {
                    Wildcard c2 = new Wildcard(filter.getTable().getSchema()
                            .getName(), filter.getTableAlias());
                    expressions.add(i++, c2);
                }
                i = temp - 1;
            } else {
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
                        if (schemaName == null ||
                                db.equalsIdentifiers(schemaName,
                                        f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,
                            tableAlias);
                }
                Table t = filter.getTable();
                String alias = filter.getTableAlias();
                expressions.remove(i);
                Column[] columns = t.getColumns();
                for (Column c : columns) {
                    if (filter.isNaturalJoinColumn(c)) {
                        continue;
                    }
                    ExpressionColumn ec = new ExpressionColumn(
                            session.getDatabase(), null, alias, c.getName());
                    expressions.add(i++, ec);
                }
                i--;
            }
        }
    }
    
    private void queryQuick(int columnCount, ResultTarget result) {
        ArrayList<Expression> expressions = prepared.getExpressions();
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }
    
    private void queryGroupSorted(int columnCount, ResultTarget result) {
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        Value[] previousKeyValues = null;
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null ||
                    Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                rowNumber++;
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    currentGroup = New.hashMap();
                }
                currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
    }
    
    
    private void addGroupSortedRow(Value[] keyValues, int columnCount,
            ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
            row[groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (groupByExpression != null && groupByExpression[j]) {
                continue;
            }
            Expression expr = expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (isHavingNullOrFalse(row)) {
            return;
        }
        row = keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }

    private void scanLevelValidation(TableFilter filter) {
        Table test = filter.getTable();
        if (!(test instanceof Table)) {
            return;
        }
        TableMate table = castTableMate(test);
        table.check();
        Index index = filter.getIndex();
        int scanLevel = table.getScanLevel();
        switch (scanLevel) {
            case TableConfig.SCANLEVEL_UNLIMITED:
                break;
            case TableConfig.SCANLEVEL_FILTER:
                if (filter.getFilterCondition() == null) {
                    throw DbException.get(ErrorCode.NOT_ALLOWED_TO_SCAN_TABLE,
                            table.getSQL(), "filter", "filter");
                }
                break;
            case TableConfig.SCANLEVEL_ANYINDEX:
                if (index.getIndexType().isScan()) {
                    throw DbException.get(ErrorCode.NOT_ALLOWED_TO_SCAN_TABLE,
                            table.getSQL(), "anyIndex", "index");
                }
                break;
            case TableConfig.SCANLEVEL_UNIQUEINDEX:
                if (!index.getIndexType().isUnique()) {
                    throw DbException.get(ErrorCode.NOT_ALLOWED_TO_SCAN_TABLE,
                            table.getSQL(), "uniqueIndex", "unique index");
                }
                break;
            case TableConfig.SCANLEVEL_SHARDINGKEY:
                if (!index.getIndexType().isShardingKey()) {
                    throw DbException.get(ErrorCode.NOT_ALLOWED_TO_SCAN_TABLE,
                            table.getSQL(), "shardingKey", "sharding key");
                }
                break;
            default:
                throw DbException.throwInternalError("error table scan level " + scanLevel);
        }
    }


    @Override
    protected List<Value> doTranslate(TableNode node, SearchRow row, StatementBuilder buff) {
        return null;
    }
}
