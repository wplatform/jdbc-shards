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
// Created on 2015年4月13日
// $Id$

package com.wplatform.ddal.shards;

import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DataSourceMarker {

    private String uid;
    private String shardName;
    private DataSource dataSource;
    private AtomicInteger failedCount = new AtomicInteger(0);
    private boolean readOnly;
    private int rWeight;
    private int wWeight;
    private boolean abnormal;

    public String getUid() {
        return uid;
    }

    public String getShardName() {
        return shardName;
    }

    /**
     * @return the datasource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * @param readOnly the readOnly to set
     */
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public int getrWeight() {
        return rWeight;
    }

    /**
     * @param rWeight the rWeight to set
     */
    public void setrWeight(int rWeight) {
        this.rWeight = rWeight;
    }

    public int getwWeight() {
        return wWeight;
    }

    /**
     * @param wWeight the wWeight to set
     */
    public void setwWeight(int wWeight) {
        this.wWeight = wWeight;
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

    /**
     * @return the abnormal
     */
    public boolean isAbnormal() {
        return abnormal;
    }

    /**
     * @param abnormal the abnormal to set
     */
    public void setAbnormal(boolean abnormal) {
        this.abnormal = abnormal;
    }


    /**
     * @param uid the uid to set
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * @param shardName the shardName to set
     */
    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    /**
     * @param dataSource the dataSource to set
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * @param failedCount the failedCount to set
     */
    public void setFailedCount(AtomicInteger failedCount) {
        this.failedCount = failedCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (abnormal ? 1231 : 1237);
        result = prime * result + ((dataSource == null) ? 0 : dataSource.hashCode());
        result = prime * result + ((failedCount == null) ? 0 : failedCount.hashCode());
        result = prime * result + rWeight;
        result = prime * result + (readOnly ? 1231 : 1237);
        result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
        result = prime * result + ((uid == null) ? 0 : uid.hashCode());
        result = prime * result + wWeight;
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
        if (abnormal != other.abnormal)
            return false;
        if (dataSource == null) {
            if (other.dataSource != null)
                return false;
        } else if (!dataSource.equals(other.dataSource))
            return false;
        if (failedCount == null) {
            if (other.failedCount != null)
                return false;
        } else if (!failedCount.equals(other.failedCount))
            return false;
        if (rWeight != other.rWeight)
            return false;
        if (readOnly != other.readOnly)
            return false;
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
        if (wWeight != other.wWeight)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DataSourceMarker [uid=" + uid + ", shardName=" + shardName + "]";
    }

    
    

}
