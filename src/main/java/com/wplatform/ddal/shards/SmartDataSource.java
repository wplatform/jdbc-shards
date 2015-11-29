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
// Created on 2015年4月19日
// $Id$

package com.wplatform.ddal.shards;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SmartDataSource implements DataSource, Failover {
    
    

    private final String shardName;
    private final DataSourceRepository database;
    private final List<DataSourceMarker> menbers;
    private final Set<DataSourceMarker> readable = New.copyOnWriteArraySet();
    private final Set<DataSourceMarker> writable = New.copyOnWriteArraySet();
    private volatile LoadBalancingStrategy writableLoadBalance;
    private volatile LoadBalancingStrategy readableLoadBalance;

    private PrintWriter out = null;
    private int seconds = 0;

    /**
     * @param uid
     * @param writable
     * @param readable
     * @param datasource
     */
    public SmartDataSource(DataSourceRepository database, String shardName, List<DataSourceMarker> menbers) {
        if (database == null) {
            throw new IllegalArgumentException("No dataSource repository specified");
        }
        if (StringUtils.isNullOrEmpty(shardName)) {
            throw new IllegalArgumentException("No shardName specified");
        }
        this.database = database;
        this.shardName = shardName;
        this.menbers = menbers;
        List<DataSourceMarker> writable = New.arrayList();
        List<DataSourceMarker> readable = New.arrayList();
        for (DataSourceMarker item : menbers) {
            if (!item.isReadOnly() && item.getwWeight() > 0) {
                writable.add(item);
            }
            if (item.getrWeight() > 0) {
                readable.add(item);
            }
        }
        if (writable.size() < 1) {
            throw new IllegalStateException();
        }
        if (readable.size() < 1) {
            throw new IllegalStateException();
        }
        this.writable.addAll(writable);
        this.readable.addAll(readable);
        this.writableLoadBalance = new ConsistentHashing(writable, false);
        this.readableLoadBalance = new ConsistentHashing(readable, true);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return out;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.out = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
    	this.seconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return seconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw DbException.getInvalidValueException("iface", iface);
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return SmartConnection.newInstance(database, this);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return SmartConnection.newInstance(database, this, username, password);
    }
    
    public DataSourceMarker doRoute(boolean readOnly) {
        DataSourceMarker next;
        if (!readOnly) {
            next = writableLoadBalance.next();
        } else {
            next = readableLoadBalance.next();
        }
        return next;
    }

    public DataSourceMarker doRoute(boolean readOnly, List<DataSourceMarker> exclusive) {
        for (DataSourceMarker marker : menbers) {
            if (exclusive.contains(marker)) {
                continue;
            }
            if (!readOnly && marker.isReadOnly()) {
                continue;
            }
            return marker;
        }
        return null;
    }

    @Override
    public void doHandleAbnormal(DataSourceMarker source) {
        if (!menbers.contains(source)) {
            throw new IllegalStateException(shardName + "datasource not matched. " + source);
        }
        if (!source.isReadOnly() && writable.remove(source)) {
            this.writableLoadBalance = new ConsistentHashing(writable, false);
        }
        if (readable.remove(source)) {
            readableLoadBalance = new ConsistentHashing(readable, true);
        }
    }

    @Override
    public void doHandleWakeup(DataSourceMarker source) {
        if (!menbers.contains(source)) {
            throw new IllegalStateException(shardName + " datasource not matched. " + source);
        }
        if (!source.isReadOnly() && source.getwWeight() > 0 && writable.add(source)) {
            this.writableLoadBalance = new ConsistentHashing(writable, false);
        }
        if (source.getrWeight() > 0 && readable.add(source)) {
            this.readableLoadBalance = new ConsistentHashing(readable, true);
        }

    }


    @Override
    public String toString() {
        return "RoutingDataSource [shardName=" + shardName + ", menbers=" + menbers + "]";
    }
    
    

}
