package com.suning.snfddal.shards;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.suning.snfddal.engine.Database;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.shards.vendor.ExceptionSorter;
import com.suning.snfddal.shards.vendor.ValidConnectionChecker;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DatabaseCluster implements DataSourceDispatcher {

    private final List<DataSourceMarker> registered;
    private final List<DataSourceMarker> abnormalList = New.copyOnWriteArrayList();
    private final List<DataSourceMarker> monitor = New.copyOnWriteArrayList();

    private final HashMap<String, DataSourceSelector> shardMaping = New.hashMap();
    private final HashMap<String, DataSourceMarker> idMapping = New.hashMap();

    private boolean traceExecution;
    private String validationQuery;
    private int validationQueryTimeout = -1;
    private ExceptionSorter recognizer;
    private ValidConnectionChecker checker;
    private final AtomicLong errorCount = new AtomicLong();
    private final Trace trace;

    public DatabaseCluster(List<DataSourceMarker> registered, Trace trace) {
        this.registered = registered;
        this.trace = trace;
        
    }

    @Override
    public Connection doDispatch(Optional option) throws SQLException {

        if (!StringUtils.isNullOrEmpty(option.dbid)) {
            return doDispatchForId(option);
        }
        DataSourceSelector selector = shardMaping.get(option.shardName);
        if (selector == null) {
            throw new SQLException(option.dbid + "shard not found.");
        }
        int retry = option.retry < 1 ? 1 : option.retry;
        SQLException lastHappened = null;
        DataSourceMarker selected = selector.doSelect(option);
        for (int i = 0; i < retry; i++) {
            if (selected == null) {
                throw new SQLException("No available datasource in shard " + option.dbid
                        + " not found.");
            }
            try {
                DataSource datasource = selected.getDataSource();
                Connection connection = datasource.getConnection();
                return connection;
            } catch (SQLException e) {
                selected.incrementFailedCount();
                monitor.add(selected);
                selected = selector.doSelect(option, monitor);
                lastHappened = e;
            }
        }
        throw lastHappened;
    }

    /**
     * @param option
     * @return
     * @throws SQLException
     */
    private Connection doDispatchForId(Optional option) throws SQLException {
        DataSourceMarker selected = idMapping.get(option.dbid);
        if (selected == null) {
            throw new SQLException("datasource " + option.dbid + " not found.");
        }
        if (!StringUtils.isNullOrEmpty(option.shardName)) {
            if (!option.shardName.equals(selected.getShardName())) {
                throw new SQLException("datasource " + option.dbid + " not found in "
                        + option.shardName + " shard.");
            }
        }
        DataSource datasource = selected.getDataSource();
        return datasource.getConnection();
    }
    
    
    private Connection getConnection(DataSourceMarker datasource, Connection conn) throws SQLException {
        if (traceExecution) {
          return SmartConnection.newInstance(this, datasource, conn);
        } else {
          return conn;
        }
      }

    /**
     * @return the validationQuery
     */
    public String getValidationQuery() {
        return validationQuery;
    }

    /**
     * @param validationQuery the validationQuery to set
     */
    public void setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
    }

    /**
     * @return the validationQueryTimeout
     */
    public int getValidationQueryTimeout() {
        return validationQueryTimeout;
    }

    /**
     * @param validationQueryTimeout the validationQueryTimeout to set
     */
    public void setValidationQueryTimeout(int validationQueryTimeout) {
        this.validationQueryTimeout = validationQueryTimeout;
    }

    public boolean isTraceExecution() {
        return traceExecution;
    }

    /**
     * @param traceExecution the traceExecution to set
     */
    public void setTraceExecution(boolean traceExecution) {
        this.traceExecution = traceExecution;
    }
    
    public Trace getTrace() {
        return trace;
    }

    public void handleException(DataSourceMarker ds, Throwable t) throws Throwable {
        errorCount.incrementAndGet();
        if (t instanceof SQLException) {
            SQLException sqlEx = (SQLException) t;
            // exceptionSorter.isExceptionFatal
            if (recognizer != null && recognizer.isExceptionFatal(sqlEx)) {
                monitor.add(ds);
            }
            throw sqlEx;
        }
        throw t;
    }

    class DataSourceMonitor extends Thread {

        DataSourceMonitor(Database db) {
            super("ha-datasource-thread");
            //trace = db.getTrace(Trace.DATASOURCE);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    for (DataSourceMarker failed : monitor) {
                        DataSource ds = failed.getDataSource();
                        try {
                            Connection conn = ds.getConnection();
                            if (checker.isValidConnection(conn, "", 1)) {

                            }
                        } catch (Exception e) {

                        }
                    }
                    for (DataSourceMarker abnorma : abnormalList) {

                    }
                    Thread.sleep(500);
                } catch (Exception e) {
                    // ignore InterruptedException
                }
            }
        }

    }

}
