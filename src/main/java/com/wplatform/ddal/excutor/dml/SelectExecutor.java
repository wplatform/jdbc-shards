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
import java.util.HashMap;
import java.util.List;

import com.wplatform.ddal.command.dml.Select;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ExpressionColumn;
import com.wplatform.ddal.config.TableConfig;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.LocalResult;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.ResultTarget;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.result.SortOrder;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.util.ValueHashMap;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueArray;
import com.wplatform.ddal.value.ValueNull;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SelectExecutor extends PreparedRoutingExecutor<Select> {
    
    protected Expression limitExpr;
    protected Expression offsetExpr;
    protected Expression sampleSizeExpr;
    protected boolean distinct;
    protected boolean randomAccessResult;
    private TableFilter topTableFilter;
    private final ArrayList<TableFilter> filters;
    private final ArrayList<TableFilter> topFilters ;
    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ArrayList<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private int havingIndex;
    private boolean isGroupQuery;
    private boolean isForUpdate;
    private boolean isQuickAggregateQuery;
    private boolean isAccordantQuery;
    private SortOrder sort;

    /**
     * @param prepared
     */
    public SelectExecutor(Select prepared) {
        super(prepared);
        limitExpr = prepared.getLimit();
        expressions = prepared.getExpressions();
        sort = prepared.getSort();
        distinct = prepared.isDistinct();
        randomAccessResult = prepared.isRandomAccessResult();
        isGroupQuery = prepared.isGroupQuery();
        isAccordantQuery = prepared.isAccordantQuery();
        isQuickAggregateQuery = prepared.isQuickAggregateQuery();
        isForUpdate = prepared.isForUpdate();
        offsetExpr = prepared.getOffset();
        topTableFilter = prepared.getTopTableFilter();
        group = prepared.getGroup();
        groupIndex = prepared.getGroupIndex();
        groupByExpression = prepared.getGroupByExpression();
        distinctColumnCount = prepared.getDistinctColumnCount();
        condition = prepared.getCondition();
        expressionArray = prepared.getExpressionArray();
        having = prepared.getHaving();
        filters = prepared.getFilters();
        topFilters = prepared.getTopFilters();
        havingIndex = prepared.getHavingIndex();
        visibleColumnCount = expressions.size();
    }


    @Override
    public LocalResult executeQuery(int maxRows, ResultTarget target) {
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
        if (sort != null) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
        }
        if (distinct) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result = createLocalResult(result);
        }
        if (isGroupQuery) {
            result = createLocalResult(result);
        }
        if (limitRows >= 0 || offsetExpr != null) {
            result = createLocalResult(result);
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        topTableFilter.lock(session, isForUpdate, isForUpdate);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if(isAccordantQuery) {
                if (isGroupQuery) {
                    queryGroupAccordant(columnCount, to);
                } else {
                    queryFlatAccordant(columnCount, to, limitRows);
                }
            } else {
                if (isGroupQuery) {
                    queryGroup(columnCount, result);
                } else {
                    queryFlat(columnCount, to, limitRows);
                }
            }
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
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


    private void queryGroupSorted(int columnCount, ResultTarget result) {
        int rowNumber = 0;
        prepared.setCurrentRowNumber(0);
        prepared.setCurrentGroup(null);
        Value[] previousKeyValues = null;
        while (topTableFilter.next()) {
            prepared.setCurrentRowNumber(rowNumber + 1);//for rownum expression
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
                    prepared.setCurrentGroup(New.<Expression, Object>hashMap());
                } else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    prepared.setCurrentGroup(New.<Expression, Object>hashMap());
                }
                prepared.increaseCurrentGroupRowId();
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

    private Value[] keepOnlyDistinct(Value[] row, int columnCount) {
        if (columnCount == distinctColumnCount) {
            return row;
        }
        // remove columns so that 'distinct' can filter duplicate rows
        Value[] r2 = new Value[distinctColumnCount];
        System.arraycopy(row, 0, r2, 0, distinctColumnCount);
        return r2;
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        if (havingIndex >= 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) {
                return true;
            }
            if (!Boolean.TRUE.equals(v.getBoolean())) {
                return true;
            }
        }
        return false;
    }

    private Index getGroupSortedIndex() {
        if (groupIndex == null || groupByExpression == null) {
            return null;
        }
        ArrayList<Index> indexes = topTableFilter.getTable().getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index.getIndexType().isScan()) {
                    continue;
                }
                if (index.getIndexType().isHash()) {
                    // does not allow scanning entries
                    continue;
                }
                if (isGroupSortedIndex(topTableFilter, index)) {
                    return index;
                }
            }
        }
        return null;
    }

    private boolean isGroupSortedIndex(TableFilter tableFilter, Index index) {
        // check that all the GROUP BY expressions are part of the index
        Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        boolean[] grouped = new boolean[indexColumns.length];
        outerLoop:
        for (int i = 0, size = expressions.size(); i < size; i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            Expression expr = expressions.get(i).getNonAliasExpression();
            if (!(expr instanceof ExpressionColumn)) {
                return false;
            }
            ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (tableFilter == exprCol.getTableFilter()) {
                    if (indexColumns[j].equals(exprCol.getColumn())) {
                        grouped[j] = true;
                        continue outerLoop;
                    }
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) {
                return false;
            }
        }
        return true;
    }

    private void queryGroup(int columnCount, LocalResult result) {
        ValueHashMap<HashMap<Expression, Object>> groups =
                ValueHashMap.newInstance();
        int rowNumber = 0;
        prepared.setCurrentRowNumber(0);
        prepared.setCurrentGroup(null);
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            prepared.setCurrentRowNumber(rowNumber + 1);
            if (condition == null ||
                    Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                } else {
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                prepared.setCurrentGroup(values);
                prepared.increaseCurrentGroupRowId();

                int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            prepared.setCurrentGroup(groups.get(key));
            Value[] keyValues = key.getList();
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
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }


    private void queryFlatAccordant(int columnCount, ResultTarget result, long limitRows) {
        
    }

    private void queryFlat(int columnCount, ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        prepared.setCurrentRowNumber(0);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            prepared.setCurrentRowNumber(rowNumber + 1);
            if (condition == null ||
                    Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
                rowNumber++;
                if ((sort == null) && limitRows > 0 &&
                        result.getRowCount() >= limitRows) {
                    break;
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
    }

    private void queryGroupAccordant(int columnCount, ResultTarget result) {
        Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }

    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressionArray,
                visibleColumnCount);
        result.done();
        return result;
    }


    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray,
                visibleColumnCount);
    }




    public String getPlanSQL() {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(
                new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (distinct) {
            buff.append(" DISTINCT");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(exprList[i].getSQL(), 4, false));
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst("\n");
                buff.append(filter.getPlanSQL(i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : topFilters) {
                do {
                    buff.appendExceptFirst("\n");
                    buff.append(f.getPlanSQL(i++ > 0));
                    f = f.getJoin();
                } while (f != null);
            }
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(
                    StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(
                    StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(
                    StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ").append(
                    sort.getSQL(exprList, visibleColumnCount));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(
                    StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(
                        StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(
                    StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    /**
     * Get the sample size, if set.
     *
     * @param session the session
     * @return the sample size
     */
    int getSampleSizeValue(Session session) {
        if (sampleSizeExpr == null) {
            return 0;
        }
        Value v = sampleSizeExpr.optimize(session).getValue(session);
        if (v == ValueNull.INSTANCE) {
            return 0;
        }
        return v.getInt();
    }


}
