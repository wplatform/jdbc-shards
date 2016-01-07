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
// Created on 2015年2月3日
// $Id$
package com.wplatform.ddal.route;

import com.wplatform.ddal.command.expression.Comparison;
import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.route.rule.RoutingArgument;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingAnalyzer {


    private Session session;
    private TableMate table;
    private List<IndexCondition> indexConditions;
    private boolean alwaysFalse;
    private SearchRow start, end;
    private ResultInterface inResult;
    private HashSet<Value> inList;

    public RoutingAnalyzer(TableMate table, List<IndexCondition> idxConds) {
        if (idxConds == null) {
            throw new IllegalArgumentException();
        }
        this.table = table;
        if (table.getRuleColumns() == null) {
            throw new IllegalArgumentException();
        }
    }

    public RoutingArgument doAnalyse(Session s, Column ruleColumn) {
        this.session = s;
        alwaysFalse = false;
        start = end = null;
        inResult = null;
        inList = null;
        // don't use enhanced for loop to avoid creating objects
        for (int i = 0, size = indexConditions.size(); i < size; i++) {
            IndexCondition condition = indexConditions.get(i);
            if (condition.isAlwaysFalse()) {
                alwaysFalse = true;
                break;
            }
            Column column = condition.getColumn();
            if (ruleColumn != column) {
                continue;
            }
            int compareType = condition.getCompareType();
            switch (compareType) {
                case Comparison.IS_NULL:
                case Comparison.EQUAL:
                case Comparison.EQUAL_NULL_SAFE:
                    Value v = condition.getCurrentValue(s);
                    inList = inList != null ? inList : New.<Value>hashSet();
                    inList.add(v);
                    break;
                case Comparison.IN_LIST:
                    inList = inList != null ? inList : New.<Value>hashSet();
                    Value[] array = condition.getCurrentValueList(s);
                    inList.addAll(Arrays.asList(array));
                    break;
                case Comparison.IN_QUERY:
                    inResult = condition.getCurrentResult();
                    break;
                default:
                    v = condition.getCurrentValue(s);
                    boolean isStart = condition.isStart();
                    boolean isEnd = condition.isEnd();
                    int columnId = column.getColumnId();
                    if (isStart) {
                        start = getSearchRow(start, columnId, v, true);
                    }
                    if (isEnd) {
                        end = getSearchRow(end, columnId, v, false);
                    }
                    if (isStart && isEnd) {
                        // a >= X and x <= X
                        inList = inList != null ? inList : New.<Value>hashSet();
                        inList.add(v);
                    }
            }
        }
        if (inResult != null) {
            while (inResult.next()) {
                Value v = inResult.currentRow()[0];
                if (v != ValueNull.INSTANCE) {
                    v = ruleColumn.convert(v);
                    inList.add(v);
                }
            }
        }

        if (inList != null) {
            return new RoutingArgument(New.arrayList(inList));
        } else if (start != null || end != null) {
            Value startV = start == null ? null : start.getValue(ruleColumn.getColumnId());
            Value endV = end == null ? null : end.getValue(ruleColumn.getColumnId());
            return new RoutingArgument(startV, endV);
        } else {
            return new RoutingArgument();
        }
    }


    private SearchRow getSearchRow(SearchRow row, int columnId, Value v, boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(row.getValue(columnId), v, max);
        }
        if (columnId < 0) {
            row.setKey(v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private Value getMax(Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        if (session.getDatabase().getSettings().optimizeIsNull) {
            // IS NULL must be checked later
            if (a == ValueNull.INSTANCE) {
                return b;
            } else if (b == ValueNull.INSTANCE) {
                return a;
            }
        }
        int comp = a.compareTo(b, table.getDatabase().getCompareMode());
        if (comp == 0) {
            return a;
        }
        if (a == ValueNull.INSTANCE || b == ValueNull.INSTANCE) {
            if (session.getDatabase().getSettings().optimizeIsNull) {
                // column IS NULL AND column <op> <not null> is always false
                return null;
            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }


}
