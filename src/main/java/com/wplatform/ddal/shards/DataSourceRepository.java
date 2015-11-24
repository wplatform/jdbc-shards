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
package com.wplatform.ddal.shards;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.wplatform.ddal.command.dml.SetTypes;
import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.config.DataSourceException;
import com.wplatform.ddal.config.DataSourceProvider;
import com.wplatform.ddal.config.ShardConfig;
import com.wplatform.ddal.config.ShardConfig.ShardItem;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.util.JdbcUtils;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DataSourceRepository {

    private final static String DEFAULT_VALIDATION_QUERY = "SELECT 1 FROM DUAL";

    private final List<DataSourceMarker> registered = New.arrayList();
    private final List<DataSourceMarker> abnormalList = New.copyOnWriteArrayList();
    private final List<DataSourceMarker> monitor = New.copyOnWriteArrayList();

    private final HashMap<String, DataSource> shardMaping = New.hashMap();
    private final HashMap<String, DataSource> idMapping = New.hashMap();

    private final DataSourceProvider dataSourceProvider;
    private final Trace trace;
    protected ScheduledExecutorService abnormalScheduler;
    protected ScheduledExecutorService monitorScheduler;
    private boolean monitorExecution;
    private String validationQuery;
    private int validationQueryTimeout = -1;
    private ScheduledExecutorService scheduledExecutor;

    public DataSourceRepository(Database database) {
        Configuration configuration = database.getConfiguration();

        this.monitorExecution = configuration.getBooleanProperty(SetTypes.MONITOR_EXECUTION, true);
        this.validationQuery = configuration.getProperty(SetTypes.VALIDATION_QUERY, null);
        this.validationQueryTimeout = configuration.getIntProperty(SetTypes.VALIDATION_QUERY_TIMEOUT, -1);

        this.dataSourceProvider = configuration.getDataSourceProvider();
        this.trace = database.getTrace(Trace.DATASOURCE);
        Map<String, ShardConfig> shardMapping = configuration.getCluster();
        for (ShardConfig value : shardMapping.values()) {
            List<ShardItem> shardItems = value.getShardItems();
            List<DataSourceMarker> shardDs = New.arrayList(shardItems.size());
            DataSourceMarker dsMarker = new DataSourceMarker();
            for (ShardItem i : shardItems) {
                String ref = i.getRef();
                DataSource dataSource = dataSourceProvider.lookup(ref);
                if (dataSource == null) {
                    throw new DataSourceException("Can' find data source: " + ref);
                }
                dsMarker.setDataSource(dataSource);
                dsMarker.setShardName(value.getName());
                dsMarker.setUid(ref);
                dsMarker.setReadOnly(i.isReadOnly());
                dsMarker.setwWeight(i.getwWeight());
                dsMarker.setrWeight(i.getrWeight());
                shardDs.add(dsMarker);
                idMapping.put(ref, dsMarker.getDataSource());
            }
            if (shardDs.size() < 1) {
                throw new DataSourceException("No datasource in " + value.getName());
            }
            registered.addAll(shardDs);
            DataSource dataSource = shardDs.size() > 1 ? new RoutingDataSource(this, value.getName(), shardDs)
                    : shardDs.get(0).getDataSource();
            shardMaping.put(value.getName(), dataSource);
        }
        scheduledExecutor = Executors.newScheduledThreadPool(1, New.customThreadFactory("datasource-ha-thread"));
        scheduledExecutor.scheduleAtFixedRate(new Worker(), 10, 10, TimeUnit.SECONDS);
    }

    public DataSource getDataSourceByShardName(String shardName) {
        DataSource dataSource = shardMaping.get(shardName);
        if(dataSource == null) {
            throw new IllegalArgumentException();
        }
        return dataSource;
    }

    public DataSource getDataSourceById(String id) {
        DataSource dataSource = idMapping.get(id);
        if(dataSource == null) {
            throw new IllegalArgumentException();
        }
        return dataSource;
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
    
    public int shardCount() {
        return this.shardMaping.size();
    }

    
    public void close() {
        try {
            this.scheduledExecutor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
    }

    Connection getConnection(DataSourceMarker selected) throws SQLException {
        DataSource dataSource = selected.getDataSource();
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            selected.incrementFailedCount();
            monitor.add(selected);
            throw e;
        }

    }

    Connection getConnection(DataSourceMarker selected, String username, String password) throws SQLException {
        DataSource dataSource = selected.getDataSource();
        try {
            return dataSource.getConnection(username, password);
        } catch (SQLException e) {
            selected.incrementFailedCount();
            monitor.add(selected);
            throw e;
        }

    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            try {
                handleMonitorList();
            } catch (Exception e) {
                trace.error(e, "datasource-ha-thread handle monitor list error");
            }
            try {
                hanldeAbnormalList();
            } catch (Exception e) {
                trace.error(e, "datasource-ha-thread handle monitor list error");
            }
        }

        /**
         * @throws SQLException
         */
        private void hanldeAbnormalList() throws SQLException {
            for (DataSourceMarker failed : abnormalList) {
                DataSource ds = failed.getDataSource();
                boolean isOk = validateAvailable(ds);
                if (isOk) {
                    DataSource dataSource = shardMaping.get(failed.getShardName());
                    Failover selector = (Failover) dataSource;
                    selector.doHandleWakeup(failed);
                    abnormalList.remove(failed);
                }

            }
        }

        /**
         * @throws SQLException
         */
        private void handleMonitorList() throws SQLException {
            for (DataSourceMarker source : monitor) {
                DataSource ds = source.getDataSource();
                boolean isOk = validateAvailable(ds);
                if (!isOk) {
                    DataSource dataSource = shardMaping.get(source.getShardName());
                    Failover selector = (Failover) dataSource;
                    selector.doHandleAbnormal(source);
                    abnormalList.add(source);
                    trace.error(null, source.toString() + " was abnormal,it's remove in " + source.getShardName());
                }
                monitor.remove(source);
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
                validationQuery = DEFAULT_VALIDATION_QUERY;
            }
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                if (validationQueryTimeout > 0) {
                    stmt.setQueryTimeout(validationQueryTimeout);
                } else {
                    stmt.setQueryTimeout(5);
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

    }

}
