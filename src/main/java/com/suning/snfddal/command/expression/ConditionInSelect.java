/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.expression;

import java.util.List;

import com.suning.snfddal.api.ErrorCode;
import com.suning.snfddal.command.dml.Query;
import com.suning.snfddal.dbobject.index.IndexCondition;
import com.suning.snfddal.dbobject.table.ColumnResolver;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.LocalResult;
import com.suning.snfddal.util.StatementBuilder;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueBoolean;
import com.suning.snfddal.value.ValueNull;

/**
 * An 'in' condition with a subquery, as in WHERE ID IN(SELECT ...)
 */
public class ConditionInSelect extends Condition {

    private final Database database;
    private Expression left;
    private final Query query;
    private final boolean all;
    private final int compareType;
    private int queryLevel;
    
    private LocalResult cachedResult;

    public ConditionInSelect(Database database, Expression left, Query query,
            boolean all, int compareType) {
        this.database = database;
        this.left = left;
        this.query = query;
        this.all = all;
        this.compareType = compareType;
    }

    @Override
    public Value getValue(Session session) {
        LocalResult rows = query(session);
        try {
            Value l = left.getValue(session);
            if (rows.getRowCount() == 0) {
                return ValueBoolean.get(all);
            } else if (l == ValueNull.INSTANCE) {
                return l;
            }
            if (!session.getDatabase().getSettings().optimizeInSelect) {
                return getValueSlow(rows, l);
            }
            if (all || (compareType != Comparison.EQUAL &&
                    compareType != Comparison.EQUAL_NULL_SAFE)) {
                return getValueSlow(rows, l);
            }
            int dataType = rows.getColumnType(0);
            if (dataType == Value.NULL) {
                return ValueBoolean.get(false);
            }
            l = l.convertTo(dataType);
            if (rows.containsDistinct(new Value[] { l })) {
                return ValueBoolean.get(true);
            }
            if (rows.containsDistinct(new Value[] { ValueNull.INSTANCE })) {
                return ValueNull.INSTANCE;
            }
            return ValueBoolean.get(false);
        } finally {
            rows.close();
        }
    }

    private Value getValueSlow(LocalResult rows, Value l) {
        // this only returns the correct result if the result has at least one
        // row, and if l is not null
        boolean hasNull = false;
        boolean result = all;
        while (rows.next()) {
            boolean value;
            Value r = rows.currentRow()[0];
            if (r == ValueNull.INSTANCE) {
                value = false;
                hasNull = true;
            } else {
                value = Comparison.compareNotNull(database, l, r, compareType);
            }
            if (!value && all) {
                result = false;
                break;
            } else if (value && !all) {
                result = true;
                break;
            }
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        query.mapColumns(resolver, level + 1);
        this.queryLevel = Math.max(level, this.queryLevel);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        query.setRandomAccessResult(true);
        query.prepare();
        if (query.getColumnCount() != 1) {
            throw DbException.get(ErrorCode.SUBQUERY_IS_NOT_SINGLE_COLUMN);
        }
        // Can not optimize: the data may change
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getSQL()).append(' ');
        if (all) {
            buff.append(Comparison.getCompareOperator(compareType)).
                append(" ALL");
        } else {
            if (compareType == Comparison.EQUAL) {
                buff.append("IN");
            } else {
                buff.append(Comparison.getCompareOperator(compareType)).
                    append(" ANY");
            }
        }
        buff.append("(\n").append(StringUtils.indent(query.getPlanSQL(), 4, false)).
            append("))");
        return buff.toString();
    }

    @Override
    public void updateAggregate(Session session) {
        left.updateAggregate(session);
        query.updateAggregate(session);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && query.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + query.getCostAsExpression();
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!session.getDatabase().getSettings().optimizeInList) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
        if (!query.isEverything(visitor)) {
            return;
        }
        filter.addIndexCondition(IndexCondition.getInQuery(l, query));
    }


    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        Session session = filter.getSession();
        LocalResult rows = query(session);
        if (rows.getRowCount() > 0) {
            StatementBuilder buff = new StatementBuilder();
            buff.append('(').append(left.exportParameters(filter,container)).append(' ');
            if (all) {
                //由于all代表全部，所以<all表示小于子查询中返回全部值中的最小值；
                //>all表示大于子查询中返回全部值中的最大值。
                buff.append(Comparison.getCompareOperator(compareType)).
                    append(" ALL");
            } else {
                if (compareType == Comparison.EQUAL) {
                    buff.append("IN");
                } else {
                    //<any可以理解为小于子查询中返回的任意一个值，因此只要小于最大值即可
                    //>any可以理解为大于子查询中返回的任意一个值，因此只要大于最小值即可
                    buff.append(Comparison.getCompareOperator(compareType)).
                        append(" ANY");
                }
            }
            buff.append("(");
            while (rows.next()) {
                buff.appendExceptFirst(",");
                buff.append("?");
                Value r = rows.currentRow()[0];
                container.add(r);
            }
            buff.append("))");
            return buff.toString();
        } else {
            return "1 = 0";
        }
        
        
    }
    
    
    private LocalResult query(Session session) {
        if(cachedResult == null) {
            query.setSession(session);
            query.setDistinct(true);
            cachedResult = query.query(0);
        } else {
            cachedResult.reset();
        }
        return cachedResult;
    }

}
