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
package com.wplatform.ddal.excutor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.shards.DataSourceRepository;
import com.wplatform.ddal.util.JdbcUtils;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class JdbcWorker<T> implements Callable<T> {
    protected final Session session;
    protected final Trace trace;
    
    protected final String shardName;
    protected final String sql;
    protected final List<Value> params;
    
    private Connection rtConn;
    private Statement rtStmt;
    private ResultSet rtRs;

    public JdbcWorker(Session session, String shardName, String sql, List<Value> params) {
        super();
        this.session = session;
        this.shardName = shardName;
        this.sql = sql;
        this.params = params;
        this.trace = session.getDatabase().getTrace(Trace.EXECUTOR);

    }

    public abstract T doWork();

    public T call() throws Exception {
        return doWork();
    }

    public void attach(Connection conn) {
        if(this.rtConn != null) {
            throw new IllegalStateException();
        }
        this.rtConn = conn;
    }

    public void attach(Statement stmt) {
        if(this.rtStmt != null) {
            throw new IllegalStateException();
        }
        this.rtStmt = stmt;
    }

    public void attach(ResultSet rs) {
        if(this.rtRs != null) {
            throw new IllegalStateException();
        }
        this.rtRs = rs;
    }
    
    
    
    /**
     * @return the rtConn
     */
    public Connection getRuntimeConnection() {
        return rtConn;
    }

    /**
     * @return the rtStmt
     */
    public Statement getRuntimeStatement() {
        return rtStmt;
    }

    
    /**
     * @return the rtRs
     */
    public ResultSet getRuntimeResultSet() {
        return rtRs;
    }

    /**
     * @return the shardName
     */
    public String getShardName() {
        return shardName;
    }

    /**
     * @return the sql
     */
    public String getSql() {
        return sql;
    }

    /**
     * @return the params
     */
    public List<Value> getParams() {
        return params;
    }

    public void cancel() {
        try {
            if(rtStmt == null) {
                return;
            }
            rtStmt.cancel();
        } catch (Exception e) {
            
        }
    }

    public void closeResource() {
        JdbcUtils.closeSilently(rtRs);
        JdbcUtils.closeSilently(rtStmt);
        JdbcUtils.closeSilently(rtConn);
    }
    
    protected void applyQueryTimeout(Statement stmt) throws SQLException {
        //The session timeout of a query in milliseconds
        int queryTimeout = session.getQueryTimeout();
        if(queryTimeout > 0) {
            int seconds = queryTimeout / 1000;
            trace.debug("apply {0} query time out from statement.", seconds);
            stmt.setQueryTimeout(seconds);
        }
    }
    

    protected DataSource getDataSource() {
        DataSourceRepository dataSourceRepository = session.getDataSourceRepository();
        DataSource dataSource = dataSourceRepository.getDataSourceByShardName(shardName);
        return dataSource;
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
