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
package com.wplatform.ddal.command.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.expression.Comparison;
import com.wplatform.ddal.command.expression.ConditionAndOr;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ExpressionColumn;
import com.wplatform.ddal.command.expression.ExpressionVisitor;
import com.wplatform.ddal.command.expression.Parameter;
import com.wplatform.ddal.command.expression.Wildcard;
import com.wplatform.ddal.dbobject.table.*;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.excutor.PreparedExecutor;
import com.wplatform.ddal.excutor.PreparedExecutorFactory;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.LocalResult;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.ResultTarget;
import com.wplatform.ddal.result.SortOrder;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;

/**
 * This class represents a simple SELECT statement.
 * <p>
 * For each select statement,
 * visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount.
 * The expression list count could include ORDER BY and GROUP BY expressions
 * that are not in the select list.
 * <p>
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {
    private final ArrayList<TableFilter> filters = New.arrayList();
    private final ArrayList<TableFilter> topFilters = New.arrayList();
    private TableFilter topTableFilter;
    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ArrayList<SelectOrderBy> orderList;
    private ArrayList<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private HashMap<Expression, Object> currentGroup;
    private int havingIndex;
    private boolean isGroupQuery;
    private boolean isForUpdate;
    private double cost;
    private boolean isPrepared, checkInit;
    private SortOrder sort;
    private int currentGroupRowId;
    private boolean isAccordantQuery;
    private boolean isQuickAggregateQuery;

    public Select(Session session) {
        super(session);
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop  if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        // Oracle doesn't check on duplicate aliases
        // String alias = filter.getAlias();
        // if (filterNames.contains(alias)) {
        //     throw Message.getSQLException(
        //         ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ArrayList<TableFilter> getTopFilters() {
        return topFilters;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    public void increaseCurrentGroupRowId() {
        ++ this.currentGroupRowId;
    }

    public void setCurrentGroup(HashMap<Expression, Object> currentGroup) {
        this.currentGroup = currentGroup;
    }

    @Override
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    private int getGroupByExpressionCount() {
        if (groupByExpression == null) {
            return 0;
        }
        int count = 0;
        for (boolean b : groupByExpression) {
            if (b) {
                ++count;
            }
        }
        return count;
    }


    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressionArray,
                visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    protected LocalResult queryWithoutCache(int maxRows, ResultTarget target)  {
        session.checkCanceled();
        PreparedExecutorFactory pef = session.getPreparedExecutorFactory();
        PreparedExecutor executor = pef.newExecutor(this);
        if(executor == null) {
            throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
        }
        return executor.executeQuery(maxRows);
    }


    private void expandColumnList() {
        Database db = session.getDatabase();

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

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ArrayList<String> expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = New.arrayList();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(session, expressions, expressionSQL, orderList,
                    visibleColumnCount, distinct, filters);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        Database db = session.getDatabase();

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            int size = group.size();
            int expSize = expressionSQL.size();
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                        sql = expr.getAlias();
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            mapColumns(f, 0);
        }
        if (havingIndex >= 0) {
            Expression expr = expressions.get(havingIndex);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0);
        }
        checkInit = true;
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            for (TableFilter f : filters) {
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
        }
        if (isGroupQuery && groupIndex == null && havingIndex < 0) {
            isQuickAggregateQuery = true;
        }
        cost = preparePlan();

        TableMate last = null;
        for (TableFilter filter : filters) {
            if (!filter.isFromTableMate()) {
                break;
            }
            if(filters.size() == 1) {
                isAccordantQuery = true;
                break;
            }
            TableMate table = (TableMate)filter.getTable();
            if(last != null && !last.isRelationSymmetry(table)) {
                break;
            }last = table;
        }
        if (distinct && session.getDatabase().getSettings().optimizeDistinct &&
                !isGroupQuery && filters.size() == 1 &&
                expressions.size() == 1 && condition == null) {
            //分布式的查询不合适
        }
        if (sort != null && !isGroupQuery) {
            
        }
        if (isGroupQuery && getGroupByExpressionCount() > 0) {
            
        }
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
        isPrepared = true;
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = New.hashSet();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() {
        TableFilter[] topArray = topFilters.toArray(
                new TableFilter[topFilters.size()]);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();

        setEvaluatableRecursive(topTableFilter);

        topTableFilter.prepare();
        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    if (session.getDatabase().getSettings().nestedJoins) {
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                            f.removeJoinCondition();
                            addCondition(on);
                        }
                    } else {
                        if (f.isJoinOuter()) {
                            // this will check if all columns exist - it may or
                            // may not throw an exception
                            on = on.optimize(session);
                            // it is not supported even if the columns exist
                            throw DbException.get(
                                    ErrorCode.UNSUPPORTED_OUTER_JOIN_CONDITION_1,
                                    on.getSQL());
                        }
                        f.removeJoinCondition();
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        addCondition(on);
                    }
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
        }
    }

    @Override
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
        if (orderList != null) {
            buff.append("\nORDER BY ");
            buff.resetCount();
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
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
        buff.append("\n/* cost: " + cost + " */");
        return buff.toString();
    }

    public Expression getHaving() {
        return having;
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    @Override
    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    @Override
    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId,
                                   int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(session, comparisonType, col, param);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(session, Comparison.EQUAL_NULL_SAFE, param, param);
        }
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                } else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    @Override
    public void updateAggregate(Session s) {
        for (Expression e : expressions) {
            e.updateAggregate(s);
        }
        if (condition != null) {
            condition.updateAggregate(s);
        }
        if (having != null) {
            having.updateAggregate(s);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
            case ExpressionVisitor.DETERMINISTIC: {
                if (isForUpdate) {
                    return false;
                }
                for (int i = 0, size = filters.size(); i < size; i++) {
                    TableFilter f = filters.get(i);
                    if (!f.getTable().isDeterministic()) {
                        return false;
                    }
                }
                break;
            }
            case ExpressionVisitor.EVALUATABLE: {
                if (!session.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                    return false;
                }
                break;
            }
            case ExpressionVisitor.GET_DEPENDENCIES: {
                for (int i = 0, size = filters.size(); i < size; i++) {
                    TableFilter f = filters.get(i);
                    Table table = f.getTable();
                    visitor.addDependency(table);
                    table.addDependencies(visitor.getDependencies());
                }
                break;
            }
            default:
        }
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        boolean result = true;
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i);
            if (!e.isEverything(v2)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(v2)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(v2)) {
            result = false;
        }
        return result;
    }

    @Override
    public boolean isReadOnly() {
        return isEverything(ExpressionVisitor.READONLY_VISITOR);
    }


    @Override
    public boolean isCacheable() {
        return !isForUpdate;
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    @Override
    public boolean allowGlobalConditions() {
        return offsetExpr == null && (limitExpr == null || sort == null);
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    //getters
    public SortOrder getSort() {
        return sort;
    }

    public ArrayList<TableFilter> getFilters() {
        return filters;
    }

    public Expression[] getExpressionArray() {
        return expressionArray;
    }

    public Expression getCondition() {
        return condition;
    }

    public int getVisibleColumnCount() {
        return visibleColumnCount;
    }

    public int getDistinctColumnCount() {
        return distinctColumnCount;
    }

    public ArrayList<SelectOrderBy> getOrderList() {
        return orderList;
    }

    public ArrayList<Expression> getGroup() {
        return group;
    }

    public int[] getGroupIndex() {
        return groupIndex;
    }

    public boolean[] getGroupByExpression() {
        return groupByExpression;
    }

    public int getHavingIndex() {
        return havingIndex;
    }

    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    public boolean isForUpdate() {
        return isForUpdate;
    }


    public boolean isPrepared() {
        return isPrepared;
    }

    public boolean isQuickAggregateQuery() {
        return isQuickAggregateQuery;
    }

    public boolean isAccordantQuery() {
        return isAccordantQuery;
    }

    
    
}
