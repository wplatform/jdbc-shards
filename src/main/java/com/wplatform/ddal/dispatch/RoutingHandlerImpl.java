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

package com.wplatform.ddal.dispatch;

import java.util.List;
import java.util.Map;

import com.wplatform.ddal.command.expression.Comparison;
import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.function.PartitionFunction;
import com.wplatform.ddal.dispatch.rule.*;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueLong;
import com.wplatform.ddal.value.ValueNull;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class RoutingHandlerImpl implements RoutingHandler {

    private Database database;
    private RoutingCalculator trc;

    public RoutingHandlerImpl(Database database) {
        this.database = database;
        this.trc = new RoutingCalculatorImpl();
    }

    @Override
    public RoutingResult doRoute(TableMate table, SearchRow row) {
        TableRouter tr = table.getTableRouter();
        if (tr != null) {
            Map<String, List<Value>> args = getRuleColumnArgs(table, row);
            RoutingResult rr = trc.calculate(tr, args);
            if (rr.isMultipleNode()) {
                throw new TableRoutingException(table.getName() + " routing error.");
            }
            return rr;
        } else {
            return fixedRoutingResult(table.getShards());
        }

    }

    @Override
    public RoutingResult doRoute(TableMate table, SearchRow first, SearchRow last) {
        TableRouter tr = table.getTableRouter();
        if (tr == null) {
            return fixedRoutingResult(table.getShards());
        } else {
            Map<String, List<Value>> routingArgs = New.hashMap();
            exportRangeArg(table, first, last, routingArgs);
            RoutingResult rr = trc.calculate(tr, routingArgs);
            return rr;
        }

    }


    @Override
    public RoutingResult doRoute(TableMate table, Session session, List<IndexCondition> indexConditions) {
        TableRouter tr = table.getTableRouter();
        if (tr == null) {
            return fixedRoutingResult(table.getShards());
        } else {
            Map<String, List<Value>> routingArgs = New.hashMap();
            List<RuleColumn> ruleCols = tr.getRuleColumns();
            SearchRow start = null, end = null;
            for (IndexCondition condition : indexConditions) {
                Column column = condition.getColumn();
                String colName = column.getName();
                RuleColumn matched = null;
                for (RuleColumn ruleColumn : ruleCols) {
                    if (colName.equalsIgnoreCase(ruleColumn.getName())) {
                        matched = ruleColumn;
                    }
                }
                if (matched == null) {
                    continue;
                }
                List<Value> values = routingArgs.get(matched.getName());
                if (values == null) {
                    values = New.arrayList();
                    routingArgs.put(matched.getName(), values);
                }
                if (condition.getCompareType() == Comparison.IN_LIST) {
                    Value[] inList = condition.getCurrentValueList(session);
                    for (Value value : inList) {
                        values.add(value);
                    }
                } else if (condition.getCompareType() == Comparison.IN_QUERY) {
                    ResultInterface result = condition.getCurrentResult();
                    while (result.next()) {
                        Value v = result.currentRow()[0];
                        if (v != ValueNull.INSTANCE) {
                            v = column.convert(v);
                            values.add(v);
                        }
                    }
                } else {
                    int columnId = column.getColumnId();
                    Value v = condition.getCurrentValue(session);
                    boolean isStart = condition.isStart();
                    boolean isEnd = condition.isEnd();
                    if (isStart) {
                        start = getSearchRow(table, session, start, columnId, v, true);
                    }
                    if (isEnd) {
                        end = getSearchRow(table, session, end, columnId, v, false);
                    }
                }
            }
            exportRangeArg(table, start, end, routingArgs);
            RoutingResult rr = trc.calculate(tr, routingArgs);
            return rr;
        }

    }

    private Map<String, List<Value>> getRuleColumnArgs(TableMate table, SearchRow row) {
        Map<String, List<Value>> args = New.hashMap();
        TableRouter tableRouter = table.getTableRouter();
        for (RuleColumn ruleCol : tableRouter.getRuleColumns()) {
            Column[] columns = table.getColumns();
            Column matched = null;
            for (Column column : columns) {
                String colName = column.getName();
                if (colName.equalsIgnoreCase(ruleCol.getName())) {
                    matched = column;
                    break;
                }
            }
            if (matched == null) {
                throw DbException.getInvalidValueException("RuleColumn", ruleCol);
            }
            Value value = row.getValue(matched.getColumnId());
            if (value != null && value != ValueNull.INSTANCE) {
                List<Value> values = New.arrayList(1);
                values.add(value);
                args.put(ruleCol.getName(), values);
            }
        }
        return args;
    }

    /**
     * @param table
     */
    private RoutingResult fixedRoutingResult(TableNode ... tableNode) {
        RoutingResult result = RoutingResult.fixedResult(tableNode);
        return result;
    }

    /**
     * @param table
     * @param first
     * @param last
     * @param routingArgs
     */
    private void exportRangeArg(TableMate table, SearchRow first, SearchRow last, Map<String, List<Value>> routingArgs) {
        TableRouter tr = table.getTableRouter();
        List<RuleColumn> ruleCols = tr.getRuleColumns();
        if (first != null && last != null) {
            for (int i = 0; first != null && i < first.getColumnCount(); i++) {
                Value firstV = first.getValue(i);
                Value listV = last.getValue(i);
                if (firstV == null || listV == null
                        || firstV == ValueNull.INSTANCE
                        || listV == ValueNull.INSTANCE) {
                    continue;
                }
                Column col = table.getColumn(i);
                String colName = col.getName();
                RuleColumn matched = null;
                for (RuleColumn ruleColumn : ruleCols) {
                    if (colName.equalsIgnoreCase(ruleColumn.getName())) {
                        matched = ruleColumn;
                    }
                }
                if (matched == null) {
                    continue;
                }
                List<Value> values = routingArgs.get(matched.getName());
                if (values == null) {
                    values = New.arrayList();
                    routingArgs.put(matched.getName(), values);
                }
                int compare = database.compare(firstV, listV);
                if (compare == 0) {
                    values.add(firstV);
                } else if (compare < 0) {
                    List<Value> enumValue = enumRange(firstV, listV);
                    if (enumValue != null) {
                        values.addAll(enumValue);
                    }
                } else {
                    throw new TableRoutingException(table.getName() + " routing error. The conidition "
                            + matched.getName() + " is alwarys false.");
                }

            }
        }
    }

    private List<Value> enumRange(Value firstV, Value listV) {
        if (firstV.getType() != listV.getType()) {
            return null;
        }
        int type = firstV.getType();
        switch (type) {
            case Value.BYTE:
            case Value.INT:
            case Value.LONG:
            case Value.SHORT:
                if (listV.subtract(firstV).getLong() > 200) {
                    return null;
                }
                List<Value> enumValues = New.arrayList(10);
                Value enumValue = firstV;
                while (database.compare(enumValue, listV) <= 0) {
                    enumValues.add(enumValue);
                    Value increase = ValueLong.get(1).convertTo(enumValue.getType());
                    enumValue = enumValue.add(increase);
                }
                return enumValues;

            default:
                return null;
        }

    }


    private SearchRow getSearchRow(TableMate table, Session s, SearchRow row, int columnId, Value v,
                                   boolean max) {
        if (row == null) {
            row = table.getTemplateRow();
        } else {
            v = getMax(table, s, row.getValue(columnId), v, max);
        }
        if (columnId < 0) {
            row.setKey(v.getLong());
        } else {
            row.setValue(columnId, v);
        }
        return row;
    }

    private Value getMax(TableMate table, Session s, Value a, Value b, boolean bigger) {
        if (a == null) {
            return b;
        } else if (b == null) {
            return a;
        }
        if (s.getDatabase().getSettings().optimizeIsNull) {
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
            if (s.getDatabase().getSettings().optimizeIsNull) {
                // column IS NULL AND column <op> <not null> is always false
                return null;
            }
        }
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }


    @Override
    public PartitionFunction getPartitionFunction(TableMate table) {
        RuleExpression ruleExpression = table.getTableRouter().getRuleExpression();
        return ruleExpression.getFunction();
    }

}
