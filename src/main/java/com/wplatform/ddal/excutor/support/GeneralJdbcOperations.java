package com.wplatform.ddal.excutor.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.JdbcOperations;
import com.wplatform.ddal.excutor.Optional;
import com.wplatform.ddal.excutor.QueryResult;
import com.wplatform.ddal.excutor.UpdateResult;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.shards.DataSourceRepository;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.Value;

public class GeneralJdbcOperations implements JdbcOperations {

    private final String shardName;
    private final Session session;
    private final DataSource dataSource;
    private final Trace trace;

    public GeneralJdbcOperations(String shardName, Session session) {
        this.shardName = shardName;
        this.session = session;
        DataSourceRepository dataSourceRepository = session.getDataSourceRepository();
        this.dataSource = dataSourceRepository.getDataSourceByShardName(shardName);
        this.trace = dataSourceRepository.getTrace();
    }

    public Session getSession() {
        return session;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public Trace getTrace() {
        return trace;
    }

    protected void applyQueryTimeout(Statement stmt) throws SQLException {
        stmt.setQueryTimeout(session.getQueryTimeout());
    }

    @Override
    public UpdateResult executeUpdate(String sql, List<Value> params) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            Optional optional = Optional.build().shardName(shardName).readOnly(false);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            conn = session.applyConnection(dataSource, optional);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {};", shardName, sql);
            }
            stmt = conn.prepareStatement(sql);
            applyQueryTimeout(stmt);
            if (params != null) {
                for (int i = 0, size = params.size(); i < size; i++) {
                    Value v = params.get(i);
                    v.set(stmt, i + 1);
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                    }
                }
            }
            int rows = stmt.executeUpdate();
            UpdateResult result = new UpdateResult(conn, stmt, rows);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} executeUpdate: {1} affected.", shardName, result.getAffectRows());
            }
            return result;
        } catch (SQLException e) {
            StatementBuilder buff = new StatementBuilder();
            buff.append(shardName).append(" executing executeUpdate error:").append(sql);
            if (params != null && params.size() > 0) {
                buff.append("\n{");
                int i = 1;
                for (Value v : params) {
                    buff.appendExceptFirst(", ");
                    buff.append(i++).append(": ").append(v.getSQL());
                }
                buff.append('}');
            }
            buff.append(';');
            trace.error(e, buff.toString());
            throw wrapException(sql, e);
        }
    }

    @Override
    public UpdateResult batchUpdate(String sql, List<Value>[] array) {
        Connection conn = null;
        PreparedStatement stmt = null;
        if (array == null || array.length < 0) {
            throw new IllegalArgumentException();
        } else if (array.length == 1) {
            return executeUpdate(sql, array[0]);
        }
        try {
            Optional optional = Optional.build().shardName(shardName).readOnly(false);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            conn = session.applyConnection(dataSource, optional);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {};", shardName, sql);
            }
            stmt = conn.prepareStatement(sql);
            applyQueryTimeout(stmt);

            for (List<Value> params : array) {
                if (params != null) {
                    for (int i = 0, size = params.size(); i < size; i++) {
                        Value v = params.get(i);
                        v.set(stmt, i + 1);
                        if (trace.isDebugEnabled()) {
                            trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                        }
                    }
                    stmt.addBatch();
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} addBatch.", shardName);
                    }
                }
            }

            int[] affected = stmt.executeBatch();
            int rows = 0;
            for (int row : affected) {
                rows += row;
            }
            UpdateResult result = new UpdateResult(conn, stmt, rows);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} executeUpdate: {1} affected.", shardName, result.getAffectRows());
            }
            return result;
        } catch (SQLException e) {
            StatementBuilder buff = new StatementBuilder();
            buff.append(shardName).append(" executing batchUpdate error:").append(sql);
            for (List<Value> params : array) {
                if (params != null) {
                    if (params != null && params.size() > 0) {
                        buff.appendExceptFirst(", ");
                        buff.append("\n{");
                        int i = 1;
                        for (Value v : params) {
                            buff.appendExceptFirst(", ");
                            buff.append(i++).append(": ").append(v.getSQL());
                        }
                        buff.append('}');
                    }
                }
            }
            buff.append(';');
            trace.error(e, buff.toString());
            throw wrapException(sql, e);
        }
    }

    @Override
    public QueryResult executeQuery(String sql, List<Value> params, int maxRows) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            Optional optional = Optional.build().shardName(shardName).readOnly(true);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            conn = session.applyConnection(dataSource, optional);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {};", shardName, sql);
            }
            stmt = conn.prepareStatement(sql);
            applyQueryTimeout(stmt);
            if (params != null) {
                for (int i = 0, size = params.size(); i < size; i++) {
                    Value v = params.get(i);
                    v.set(stmt, i + 1);
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                    }
                }
            }
            return new QueryResult(conn, stmt, stmt.executeQuery());
        } catch (SQLException e) {
            StatementBuilder buff = new StatementBuilder();
            buff.append(shardName).append(" executing executeQuery error:").append(sql);
            if (params != null && params.size() > 0) {
                buff.append("\n{");
                int i = 1;
                for (Value v : params) {
                    buff.appendExceptFirst(", ");
                    buff.append(i++).append(": ").append(v.getSQL());
                }
                buff.append('}');
            }
            buff.append(';');
            trace.error(e, buff.toString());
            throw wrapException(sql, e);
        }
    }
    
    /**
     * Wrap a SQL exception that occurred while accessing a linked table.
     *
     * @param sql the SQL statement
     * @param ex the exception from the remote database
     * @return the wrapped exception
     */
    protected static DbException wrapException(String sql, Exception ex) {
        SQLException e = DbException.toSQLException(ex);
        return DbException.get(ErrorCode.ERROR_ACCESSING_DATABASE_TABLE_2, e, sql, e.toString());
    }

}
