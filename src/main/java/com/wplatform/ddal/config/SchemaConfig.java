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
// Created on 2015年3月31日
// $Id$

package com.wplatform.ddal.config;

import java.io.Serializable;
import java.util.List;

import com.wplatform.ddal.util.New;

public class SchemaConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private String shard;
    private boolean validation;
    private List<TableConfig> tables = New.arrayList();

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the shard
     */
    public String getShard() {
        return shard;
    }

    /**
     * @param shard the shard to set
     */
    public void setShard(String shard) {
        this.shard = shard;
    }

    /**
     * @return the tables
     */
    public List<TableConfig> getTables() {
        return tables;
    }

    /**
     * @param tables the tables to set
     */
    public void setTables(List<TableConfig> tables) {
        this.tables = tables;
    }

    /**
     * @return the validation
     */
    public boolean isValidation() {
        return validation;
    }

    /**
     * @param validation the validation to set
     */
    public void setValidation(boolean validation) {
        this.validation = validation;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        SchemaConfig other = (SchemaConfig) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }

}