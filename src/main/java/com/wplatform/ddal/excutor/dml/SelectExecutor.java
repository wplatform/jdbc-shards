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
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.LocalResult;
import com.wplatform.ddal.result.ResultTarget;
import com.wplatform.ddal.result.SortOrder;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

import java.util.ArrayList;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class SelectExecutor extends CommonPreparedExecutor<Select> {

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

    private void scanLevelValidation(TableFilter filter) {
        Table test = filter.getTable();
        if(!(test instanceof Table)) {
            return;
        }
        TableMate table = castTableMate(test);
        Index index = filter.getIndex();
        int scanLevel = table.getScanLevel();
        switch (scanLevel) {
            case TableConfig.SCANLEVEL_UNLIMITED:
                break;
            case TableConfig.SCANLEVEL_FILTER:
                if(filter.getFilterCondition() == null) {
                    throw DbException.get(ErrorCode.CONDITION_NOT_ALLOWED_FOR_scan_TABLE,table.getSQL());
                }
                break;
            case TableConfig.SCANLEVEL_ANYINDEX:
                if(index.getIndexType().isScan()) {
                    throw DbException.get(ErrorCode.CONDITION_NOT_ALLOWED_FOR_scan_TABLE,table.getSQL());
                }
                break;
            case TableConfig.SCANLEVEL_UNIQUEINDEX:
                if(!index.getIndexType().isUnique()) {
                    throw DbException.get(ErrorCode.CONDITION_NOT_ALLOWED_FOR_scan_TABLE,table.getSQL());
                }
                break;
            case TableConfig.SCANLEVEL_SHARDINGKEY:
                if(!index.getIndexType().isShardingKey()) {
                    throw DbException.get(ErrorCode.CONDITION_NOT_ALLOWED_FOR_scan_TABLE,table.getSQL());
                }
                break;
            default: throw DbException.throwInternalError("error table scan level " + scanLevel);
        }
    }
}
