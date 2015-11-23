package com.wplatform.ddal.excutor.spi;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.wplatform.ddal.dbobject.index.ResultSetCursor;
import com.wplatform.ddal.excutor.JdbcOperations;
import com.wplatform.ddal.value.Value;

public class GeneralJdbcOperations implements JdbcOperations {

    protected static final Log logger = LogFactory.getLog(JdbcOperations.class);

    private DataSource dataSource;
    /**
     * If this variable is set to a non-negative value, it will be used for
     * setting the fetchSize property on statements used for query processing.
     */
    private int fetchSize = -1;

    /**
     * If this variable is set to a non-negative value, it will be used for
     * setting the maxRows property on statements used for query processing.
     */
    private int maxRows = -1;

    /**
     * If this variable is set to a non-negative value, it will be used for
     * setting the queryTimeout property on statements used for query
     * processing.
     */
    private int queryTimeout = -1;
    
    

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    @Override
    public int executeUpdate(String sql) {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, List<Value> params) {
        return 0;
    }

    @Override
    public ResultSetCursor executeQuery(String sql) {
        return null;
    }

    @Override
    public ResultSetCursor executeQuery(String sql, List<Value> params) {
        return null;
    }

    @Override
    public int[] batchUpdate(String[] sql) {
        return null;
    }

    @Override
    public int[] batchUpdate(String sql, List<Value>[] array) {
        // TODO Auto-generated method stub
        return null;
    }
    
    
    protected void applyStatementSettings(Statement stmt) throws SQLException {
        int fetchSize = getFetchSize();
        if (fetchSize >= 0) {
            stmt.setFetchSize(fetchSize);
        }
        int maxRows = getMaxRows();
        if (maxRows >= 0) {
            stmt.setMaxRows(maxRows);
        }
        //DataSourceUtils.applyTimeout(stmt, getDataSource(), getQueryTimeout());
    }

}
