/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年4月13日
// $Id$

package com.suning.snfddal.shard;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.suning.snfddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class UidDataSource implements DataSource {

    private final String uid;

    private final boolean writable;

    private final boolean readable;

    private final DataSource targetDatasource;

    /**
     * @param uid
     * @param writable
     * @param readable
     * @param datasource
     */
    public UidDataSource(String uid, boolean writable, boolean readable, DataSource datasource) {
        if(StringUtils.isNullOrEmpty(uid)) {
            throw new IllegalArgumentException("No uid specified");
        }
        if(datasource == null) {
            throw new IllegalArgumentException("No DataSource specified");
        }
        this.uid = uid;
        this.writable = writable;
        this.readable = readable;
        this.targetDatasource = datasource;
    }

    /**
     * @return the xid
     */
    public String getUid() {
        return uid;
    }

    /**
     * @return the writable
     */
    public boolean isWritable() {
        return writable;
    }

    /**
     * @return the readable
     */
    public boolean isReadable() {
        return readable;
    }

    /**
     * @return the datasource
     */
    public DataSource getTargetDataSource() {
        return targetDatasource;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return targetDatasource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        targetDatasource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        targetDatasource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return targetDatasource.getLoginTimeout();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("Invalid iface " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    @Override
    public Connection getConnection() throws SQLException {
        return targetDatasource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return targetDatasource.getConnection(username, password);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uid == null) ? 0 : uid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UidDataSource other = (UidDataSource) obj;
        if (uid == null) {
            if (other.uid != null)
                return false;
        } else if (!uid.equals(other.uid))
            return false;
        return true;
    }
    
    
    

}
