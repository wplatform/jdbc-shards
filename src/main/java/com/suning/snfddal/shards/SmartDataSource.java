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
// Created on 2015年4月19日
// $Id$

package com.suning.snfddal.shards;

import com.suning.snfddal.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SmartDataSource implements DataSourceMarker {

    private final DataSourceRepository dsMgr;

    private final String uid;
    private final String shardName;
    private final DataSource dataSource;
    private final AtomicInteger failedCount = new AtomicInteger(0);
    private boolean readOnly;
    private int rWeight;
    private int wWeight;

    /**
     * @param uid
     * @param writable
     * @param readable
     * @param datasource
     */
    public SmartDataSource(DataSourceRepository dsMgr, String uid, String shardName, DataSource datasource) {
        if (dsMgr == null) {
            throw new IllegalArgumentException("No SmartDataSourceManager specified");
        }
        if (StringUtils.isNullOrEmpty(uid)) {
            throw new IllegalArgumentException("No uid specified");
        }
        if (StringUtils.isNullOrEmpty(shardName)) {
            throw new IllegalArgumentException("No shardName specified");
        }
        if (datasource == null) {
            throw new IllegalArgumentException("No DataSource specified");
        }
        this.dsMgr = dsMgr;
        this.uid = uid;
        this.shardName = shardName;
        this.dataSource = datasource;
    }

    @Override
    public String getUid() {
        return uid;
    }

    @Override
    public String getShardName() {
        return shardName;
    }

    /**
     * @return the datasource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * @param readOnly the readOnly to set
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public int getrWeight() {
        return rWeight;
    }

    /**
     * @param rWeight the rWeight to set
     */
    public void setrWeight(int rWeight) {
        this.rWeight = rWeight;
    }

    @Override
    public int getwWeight() {
        return wWeight;
    }

    /**
     * @param wWeight the wWeight to set
     */
    public void setwWeight(int wWeight) {
        this.wWeight = wWeight;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
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
        SmartDataSource other = (SmartDataSource) obj;
        if (shardName == null) {
            if (other.shardName != null)
                return false;
        } else if (!shardName.equals(other.shardName))
            return false;
        if (uid == null) {
            if (other.uid != null)
                return false;
        } else if (!uid.equals(other.uid))
            return false;
        return true;
    }

    /**
     * @return
     * @see java.util.concurrent.atomic.AtomicInteger#get()
     */
    public final int getFailedCount() {
        return failedCount.get();
    }

    /**
     * @param newValue
     * @see java.util.concurrent.atomic.AtomicInteger#set(int)
     */
    public final void resetFailedCount() {
        failedCount.set(0);
    }

    /**
     * @return
     * @see java.util.concurrent.atomic.AtomicInteger#incrementAndGet()
     */
    public final void incrementFailedCount() {
        failedCount.incrementAndGet();
    }

    @Override
    public String toString() {
        return "[" + uid + "]";
    }

    @Override
    public Connection doGetConnection() throws SQLException {
        return dsMgr.getConnection(this);
    }

}
