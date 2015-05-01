package com.suning.snfddal.shards;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import com.suning.snfddal.command.dml.SetTypes;
import com.suning.snfddal.config.Configuration;
import com.suning.snfddal.config.DataSourceException;
import com.suning.snfddal.config.DataSourceProvider;
import com.suning.snfddal.config.ShardConfig;
import com.suning.snfddal.config.ShardConfig.ShardItem;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Mode;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.shards.vendor.DB2ExceptionSorter;
import com.suning.snfddal.shards.vendor.MySqlExceptionSorter;
import com.suning.snfddal.shards.vendor.NullExceptionSorter;
import com.suning.snfddal.shards.vendor.OracleExceptionSorter;
import com.suning.snfddal.util.JdbcUtils;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.util.Utils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DataSourceRepository implements DataSourceDispatcher {

    public final static String DEFAULT_VALIDATION_QUERY = null;

    private final List<SmartDataSource> registered = New.arrayList();
    private final List<SmartDataSource> abnormalList = New.copyOnWriteArrayList();
    private final List<SmartDataSource> monitor = New.copyOnWriteArrayList();

    private final HashMap<String, DataSourceSelector> shardMaping = New.hashMap();
    private final HashMap<String, SmartDataSource> idMapping = New.hashMap();

    private final DataSourceProvider dataSourceProvider;
    private boolean monitorExecution;
    private String validationQuery;
    private int validationQueryTimeout = -1;
    private ExceptionSorter recognizer;
    private final AtomicLong errorCount = new AtomicLong();
    private final Trace trace;
    protected ScheduledExecutorService abnormalScheduler;
    protected ScheduledExecutorService monitorScheduler;

    public DataSourceRepository(Database database) {
        Configuration configuration = database.getConfiguration();

        this.monitorExecution = configuration.getBooleanProperty(SetTypes.MONITOR_EXECUTION, true);
        this.validationQuery = configuration.getProperty(SetTypes.VALIDATION_QUERY, null);
        this.validationQueryTimeout = configuration.getIntProperty(
                SetTypes.VALIDATION_QUERY_TIMEOUT, -1);
        String recognizerName = configuration.getProperty(SetTypes.EXCEPTION_SORTER_CLASS, null);
        
        if (!StringUtils.isNullOrEmpty(recognizerName)) {
            setExceptionSorter(recognizerName);
        } else {
            Mode sqlMode = database.getMode();
            if (Mode.MY_SQL.equals(sqlMode.getName())) {
                recognizer = new MySqlExceptionSorter();
            } else if (Mode.ORACLE.equals(sqlMode.getName())) {
                recognizer = new OracleExceptionSorter();
            } else if (Mode.DB2.equals(sqlMode.getName())) {
                recognizer = new DB2ExceptionSorter();
            } else {
                recognizer = new NullExceptionSorter();
            }
        }

        this.dataSourceProvider = configuration.getDataSourceProvider();
        this.trace = database.getTrace(Trace.DATASOURCE);
        Map<String, ShardConfig> shardMapping = configuration.getCluster();
        for (ShardConfig value : shardMapping.values()) {
            List<ShardItem> shardItems = value.getShardItems();
            List<SmartDataSource> shardDs = New.arrayList(shardItems.size());
            SmartDataSource ds;
            for (ShardItem i : shardItems) {
                String ref = i.getRef();
                DataSource dataSource = dataSourceProvider.lookup(ref);
                if (dataSource == null) {
                    throw new DataSourceException("Can' find data source: " + ref);
                }
                ds = new SmartDataSource(this, ref, value.getName(), dataSource);
                ds.setReadOnly(i.isReadOnly());
                ds.setwWeight(i.getwWeight());
                ds.setrWeight(i.getrWeight());
                shardDs.add(ds);
                idMapping.put(ref, ds);
            }
            registered.addAll(shardDs);
            DataSourceSelector selector = DataSourceSelector.create(value.getName(), shardDs);
            shardMaping.put(value.getName(), selector);
        }

        DataSourceMonitor monitor = new DataSourceMonitor(database);
        monitor.start();
    }

    @Override
    public SmartDataSource doDispatch(Optional option) throws SQLException {
        if (!StringUtils.isNullOrEmpty(option.dbid)) {
            return doDispatchForId(option);
        }
        DataSourceSelector selector = shardMaping.get(option.shardName);
        if (selector == null) {
            throw new SQLException(option.dbid + "shard not found.");
        }
        int retry = option.retry < 1 ? 1 : option.retry;
        SmartDataSource selected = selector.doSelect(option);
        for (int i = 0; i < retry; i++) {
            if (selected != null) {
                return selected;
            } else {
                selected = selector.doSelect(option, monitor);
            }
        }
        if (selected == null) {
            throw new SQLException("No available datasource in shard " + option.shardName);
        }
        // lastHappened is ever not null
        return selected;
    }

    /**
     * @param option
     * @return
     * @throws SQLException
     */
    private SmartDataSource doDispatchForId(Optional option) throws SQLException {
        SmartDataSource selected = idMapping.get(option.dbid);
        if (selected == null) {
            throw new SQLException("datasource " + option.dbid + " not found.");
        }
        if (!StringUtils.isNullOrEmpty(option.shardName)) {
            if (!option.shardName.equals(selected.getShardName())) {
                throw new SQLException("datasource " + option.dbid + " not found in "
                        + option.shardName + " shard.");
            }
        }
        return selected;
    }

    Connection getConnection(SmartDataSource selected) throws SQLException {
        DataSource dataSource = selected.getDataSource();
        try {
            Connection conn = dataSource.getConnection();
            if (monitorExecution) {
                return SmartConnection.newInstance(this, selected, conn);
            } else {
                return conn;
            }
        } catch (SQLException e) {
            selected.incrementFailedCount();
            monitor.add(selected);
            throw e;
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
        return monitorExecution;
    }

    /**
     * @param traceExecution the traceExecution to set
     */
    public void setTraceExecution(boolean traceExecution) {
        this.monitorExecution = traceExecution;
    }

    public Trace getTrace() {
        return trace;
    }

    public void handleException(SmartDataSource ds, Throwable t) throws Throwable {
        errorCount.incrementAndGet();
        if (t instanceof SQLException) {
            SQLException sqlEx = (SQLException) t;
            // exceptionSorter.isExceptionFatal
            if (recognizer != null && recognizer.isExceptionFatal(sqlEx)) {
                ds.incrementFailedCount();
                monitor.add(ds);
            }
            throw sqlEx;
        }
        throw t;
    }

    private void setExceptionSorter(String exceptionSorter) {
        exceptionSorter = exceptionSorter.trim();
        if (exceptionSorter.length() == 0) {
            this.recognizer = NullExceptionSorter.getInstance();
            return;
        }
        Class<?> clazz = Utils.loadClass(exceptionSorter);
        if (clazz != null) {
            try {
                this.recognizer = (ExceptionSorter) clazz.newInstance();
            } catch (Exception ex) {
                DbException.convert(ex);
            }
        }
    }

    private boolean validateAvailable(DataSource dataSource) throws SQLException {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException ex) {
            // skip
            return false;
        }
        if (validationQuery == null) {
            return true;
        }
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            if (validationQueryTimeout > 0) {
                stmt.setQueryTimeout(validationQueryTimeout);
            }
            rs = stmt.executeQuery(validationQuery);
            return true;
        } catch (SQLException e) {
            return false;
        } catch (Exception e) {
            // LOG.warn("Unexpected error in ping", e);
            return false;
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
        }

    }

   private class DataSourceMonitor extends Thread {
       private DataSourceMonitor(Database db) {
            super("datasource-monitor-thread");
        }

        @Override
        public void run() {
            while (true) {
                try {
                    for (SmartDataSource source : monitor) {
                        DataSource ds = source.getDataSource();
                        boolean isOk = validateAvailable(ds);
                        if (!isOk) {
                            DataSourceSelector selector = shardMaping.get(source.getShardName());
                            selector.doHandleAbnormal(source);
                            abnormalList.add(source);
                            trace.error(null, source.toString() + " was abnormal,it's remove in "
                                    + source.getShardName());
                        }
                        monitor.remove(source);
                    }
                    for (SmartDataSource failed : abnormalList) {
                        DataSource ds = failed.getDataSource();
                        boolean isOk = validateAvailable(ds);
                        if (isOk) {
                            DataSourceSelector selector = shardMaping.get(failed.getShardName());
                            selector.doHandleWakeup(failed);
                            abnormalList.remove(failed);
                        }

                    }
                    Thread.sleep(500);
                } catch (Exception e) {
                    trace.error(e, "datasource-monitor-thread error");
                }
            }
        }

    }

}
