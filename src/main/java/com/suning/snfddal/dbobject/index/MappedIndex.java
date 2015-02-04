/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.suning.snfddal.command.dml.Select;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.command.expression.Parameter;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.dbobject.table.MappedTable;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Constants;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.result.SortOrder;
import com.suning.snfddal.route.MultiNodeExecutor;
import com.suning.snfddal.route.RoutingHandler;
import com.suning.snfddal.route.TableRoutingException;
import com.suning.snfddal.route.rule.RoutingResult;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StatementBuilder;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class MappedIndex extends BaseIndex {
    
    private static final Pattern ARG_PATTERN = Pattern.compile("\\?[0-9]+");

    private final MappedTable mappedTable;
    private final String targetTableName;
    private long rowCount;
    
    private RoutingHandler routingHandler;

    public MappedIndex(MappedTable table, int id, IndexColumn[] columns,
            IndexType indexType) {
        initBaseIndex(table, id, null, columns, indexType);
        this.routingHandler = table.getSchema().getDatabase().getRoutingHandler();
        mappedTable = table;
        targetTableName = mappedTable.getQualifiedTable();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    private static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    @Override
    public void add(Session session, Row row) {
        RoutingResult result = routingHandler.doRoute(mappedTable, row);
        List<RoutingResult.MatchedShard> shards = result.getMatchedShards();
        if (shards.size() != 1 && shards.get(0).getTables().length != 1) {
            throw new TableRoutingException(table.getName() + " routing error.");
        }
        String shardName = shards.get(0).getShardName();
        String tableName = shards.get(0).getTables()[0];
        ArrayList<Value> params = New.arrayList();
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(tableName);
        buff.append(" (").append(buildColumnList()).append(")");
        buff.append(" VALUES(");
        for (int i = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            buff.appendExceptFirst(", ");
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            mappedTable.execute(session, shardName, sql, params, true);
            rowCount++;
        } catch (Exception e) {
            throw MappedTable.wrapException(sql, e);
        }
    }
    
    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        throw DbException.getUnsupportedException("Unimplement method.");
    }
    
    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        ArrayList<Value> queryParams = New.arrayList();
        String queryCondition = buildQueryConditon(filter, queryParams);
        Session session = filter.getSession();
        List<IndexCondition> conditions = filter.getIndexConditions();
        RoutingResult rr = routingHandler.doRoute(mappedTable, session, conditions);
        List<RoutingResult.MatchedShard> shards = rr.getMatchedShards();
        List<Callable<ResultCursor>> callables = New.arrayList(shards.size());
        
        String shardName = null;
        String sql = null;
        ArrayList<Value> params = null;
        for (RoutingResult.MatchedShard shard : shards) {
            StatementBuilder shardSql = new StatementBuilder();
            shardName = shard.getShardName();
            params = New.arrayList();
            String[] tables = shard.getTables();
            if(tables.length == 0) {
                sql = buildQuerySqlFromTable(filter, targetTableName, queryCondition);
                params.addAll(queryParams);
            } else if(tables.length == 1){
                sql = buildQuerySqlFromTable(filter, tables[0], queryCondition);
                params.addAll(queryParams);
            }else {
                shardSql.append("SELECT * FROM ( ");
                for (String table : tables) {
                    shardSql.appendExceptFirst(" UNION ALL ");
                    shardSql.append(buildQuerySqlFromTable(filter, table, queryCondition));
                    params.addAll(queryParams);
                }
                shardSql.append(" ) ").append(mappedTable.getName());
                sql =shardSql.toString();
            }
            callables.add(newQueryCallable(session, shardName, sql, params));
        }
        if(callables.size() > 1) {
           List<ResultCursor> results = MultiNodeExecutor.execute(callables);
           return new MergedCursor(results);
        } else if(callables.size() == 1) {
            return find(session, shardName, sql, params);
        } else {
            throw DbException.throwInternalError();
        }
    
    }

    public ResultCursor find(Session session, String shardName, String sql, List<Value> params) {
        try {
            PreparedStatement prep = mappedTable.execute(session, shardName, sql, params, false);
            ResultSet rs = prep.getResultSet();
            return new ResultCursor(mappedTable, rs, session);
        } catch (Exception e) {
            throw MappedTable.wrapException(sql, e);
        }
    }

    private void addParameter(StatementBuilder buff, Column col) {
        if (col.getType() == Value.STRING_FIXED && mappedTable.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            buff.append("CAST(? AS CHAR(").append(col.getPrecision()).append("))");
        } else {
            buff.append('?');
        }
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter,
            SortOrder sortOrder) {
        return 100 + getCostRangeIndex(masks, rowCount +
                Constants.COST_ROW_OFFSET, filter, sortOrder);
    }

    @Override
    public void remove(Session session) {
        // nothing to do
    }

    @Override
    public void truncate(Session session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        // TODO optimization: could get the first or last value (in any case;
        // maybe not optimized)
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public void remove(Session session, Row row) {
        RoutingResult result = routingHandler.doRoute(mappedTable, row);
        List<RoutingResult.MatchedShard> shards = result.getMatchedShards();
        if (shards.size() != 1 && shards.get(0).getTables().length != 1) {
            throw new TableRoutingException(table.getName() + " routing error.");
        }
        String shardName = shards.get(0).getShardName();
        String tableName = shards.get(0).getTables()[0];
        ArrayList<Value> params = New.arrayList();
        StatementBuilder buff = new StatementBuilder("DELETE FROM ");
        buff.append(tableName).append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            buff.appendExceptFirst("AND ");
            Column col = table.getColumn(i);
            buff.append(col.getSQL());
            Value v = row.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            } else {
                buff.append('=');
                addParameter(buff, col);
                params.add(v);
                buff.append(' ');
            }
        }
        String sql = buff.toString();
        try {
            PreparedStatement prep = mappedTable.execute(session, shardName, sql, params, false);
            int count = prep.executeUpdate();
            mappedTable.reusePreparedStatement(prep, sql);
            rowCount -= count;
        } catch (Exception e) {
            throw MappedTable.wrapException(sql, e);
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the
     * emit updates option is enabled.
     *
     * @param oldRow the old data
     * @param newRow the new data
     */
    public void update(Session session, Row oldRow, Row newRow) {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }
    
    /**
     * @param columnList
     * @param connditionSql
     */
    private String buildQuerySqlFromTable(TableFilter tf, String tableName, String queryConndition) {
        Select select = tf.getSelect();
        StatementBuilder sql = new StatementBuilder();
        sql.append("SELECT ");
        if(select.isDistinct()) {
            sql.append("DISTINCT ");
        }
        sql.append(buildColumnList());
        sql.append(" FROM ").append(tableName);
        if(!StringUtils.isNullOrEmpty(tf.getTableAlias())) {
            sql.append(" ").append(tf.getTableAlias());
        }
        if(!StringUtils.isNullOrEmpty(queryConndition)) {
            sql.append(" WHERE ").append(queryConndition);
        }
        /*
        Expression limitExpr = select.getLimit();
        Expression offsetExpr = select.getOffset();
        if (limitExpr != null) {
            sql.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                sql.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }*/
        return sql.toString();
    }
    
    private String buildColumnList() {
        StatementBuilder string = new StatementBuilder();
        for (Column col : columns) {
            string.appendExceptFirst(",");
            string.append(col.getName());            
        }
        return string.toString();
    }
    
    private String buildQueryConditon(TableFilter tf, List<Value> params) {
        Expression conditionExpr = tf.getFilterCondition();
        String condition = null;
        if(conditionExpr != null) {
            condition = conditionExpr.exportParameters(tf, params);
            condition = StringUtils.unEnclose(condition);
        }
        return condition;
    }
    
    protected String exportSQLParameter(String sql,List<Parameter> parms, List<Value> container) {
        Matcher matcher = ARG_PATTERN.matcher(sql);
        boolean isFind = false;
        while (matcher.find()) {
            String group = matcher.group();
            String paramIndex = group.substring(1,group.length());
            int index = Integer.parseInt(paramIndex);
            Parameter parm = parms.get(index - 1);
            container.add(parm.getParamValue());
            isFind = true;
        }
        if(isFind) {
            sql = sql.replaceAll(ARG_PATTERN.pattern(), "?");
        }
       return sql;
    }
    
    private Callable<ResultCursor> newQueryCallable(
            final Session session, 
            final String shardName, 
            final String sql,
            final List<Value> params) {
        Callable<ResultCursor> call = new Callable<ResultCursor>() {
            @Override
            public ResultCursor call() throws Exception {
                return find(session, shardName, sql, params);
            }
        };
        return call;
    }
    
    
    
}
