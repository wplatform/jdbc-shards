/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
// Created on 2015年3月27日
// $Id$

package com.suning.snfddal.dispatch.rule;

import com.suning.snfddal.util.StringUtils;

import java.io.Serializable;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String shardName;

    private final String tableName;

    private final String suffix;

    /**
     * @param shardName
     * @param tableName
     */
    public TableNode(String shardName, String tableName) {
        this(shardName, tableName, null);
    }

    public TableNode(String shardName, String tableName, String suffix) {
        this.shardName = shardName;
        this.tableName = tableName;
        this.suffix = suffix;
    }

    /**
     * @return the shardName
     */
    public String getShardName() {
        return shardName;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return the suffix
     */
    public String getSuffix() {
        return suffix;
    }


    public String getCompositeTableName() {
        StringBuilder fullName = new StringBuilder();
        fullName.append(tableName);
        if (!StringUtils.isNullOrEmpty(suffix)) {
            fullName.append(suffix);
        }
        return fullName.toString();
    }
    
    
    

    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
        result = prime * result + ((suffix == null) ? 0 : suffix.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableNode other = (TableNode) obj;
        if (shardName == null) {
            if (other.shardName != null)
                return false;
        } else if (!shardName.equals(other.shardName))
            return false;
        if (suffix == null) {
            if (other.suffix != null)
                return false;
        } else if (!suffix.equals(other.suffix))
            return false;
        if (tableName == null) {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getShardName() + "." + getCompositeTableName();
    }

    

}
