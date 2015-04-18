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

package com.suning.snfddal.shards;

import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.suning.snfddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DataSourceMarker {
    
    private final String uid;

    private final String shardName;
    
    private final boolean writable;

    private final boolean readable;

    private final DataSource dataSource;
    
    private final AtomicInteger failedCount = new AtomicInteger(0);

    /**
     * @param uid
     * @param writable
     * @param readable
     * @param datasource
     */
    public DataSourceMarker(String uid, String shardName,boolean writable, boolean readable, DataSource datasource) {
        if(StringUtils.isNullOrEmpty(uid)) {
            throw new IllegalArgumentException("No uid specified");
        }
        if(StringUtils.isNullOrEmpty(shardName)) {
            throw new IllegalArgumentException("No shardName specified");
        }
        if(datasource == null) {
            throw new IllegalArgumentException("No DataSource specified");
        }
        this.uid = uid;
        this.shardName = shardName;
        this.writable = writable;
        this.readable = readable;
        this.dataSource = datasource;
    }

    /**
     * @return the xid
     */
    public String getUid() {
        return uid;
    }

    /**
     * @return the shardName
     */
    public String getShardName() {
        return shardName;
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
    public DataSource getDataSource() {
        return dataSource;
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
        DataSourceMarker other = (DataSourceMarker) obj;
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

    
    
    
    


}
