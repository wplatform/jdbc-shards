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
// Created on 2015年3月27日
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.io.Serializable;

import com.wplatform.ddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String shardName;

    private final String objectName;

    private final String suffix;

    /**
     * @param shardName
     * @param objectName
     */
    public TableNode(String shardName, String objectName) {
        this(shardName, objectName, null);
    }

    public TableNode(String shardName, String objectName, String suffix) {
        this.shardName = shardName;
        this.objectName = objectName;
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
    public String getObjectName() {
        return objectName;
    }

    /**
     * @return the suffix
     */
    public String getSuffix() {
        return suffix;
    }


    public String getCompositeObjectName() {
        StringBuilder fullName = new StringBuilder();
        fullName.append(objectName);
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
        result = prime * result + ((objectName == null) ? 0 : objectName.hashCode());
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
        if (objectName == null) {
            if (other.objectName != null)
                return false;
        } else if (!objectName.equals(other.objectName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getShardName() + "." + getCompositeObjectName();
    }

    

}
