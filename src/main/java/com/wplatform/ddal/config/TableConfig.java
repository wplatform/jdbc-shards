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

import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.dispatch.rule.TableRouter;
import com.wplatform.ddal.engine.Constants;

public class TableConfig {

    public static final int SCANLEVEL_UNLIMITED = 1;
    public static final int SCANLEVEL_FILTER = 2;
    public static final int SCANLEVEL_ANYINDEX = 3;
    public static final int SCANLEVEL_UNIQUEINDEX = 4;
    public static final int SCANLEVEL_SHARDINGKEY = 5;

    private SchemaConfig schemaConfig;
    private String name;    
    private int scanLevel = SCANLEVEL_ANYINDEX;
    private boolean validation;
    private TableNode[] shards;
    private TableRouter tableRouter;

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
     * @return the schemaConfig
     */
    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    /**
     * @param schemaConfig the schemaConfig to set
     */
    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
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

    /**
     * @return the shards
     */
    public TableNode[] getShards() {
        return shards;
    }

    /**
     * @param shards the shards to set
     */
    public void setShards(TableNode[] shards) {
        this.shards = shards;
    }

    /**
     * @return the scanLevel
     */
    public int getScanLevel() {
        return scanLevel;
    }

    /**
     * @param scanLevel the scanLevel to set
     */
    public void setScanLevel(int scanLevel) {
        this.scanLevel = scanLevel;
    }

    /**
     * @return the tableRouter
     */
    public TableRouter getTableRouter() {
        return tableRouter;
    }

    /**
     * @param tableRouter the tableRouter to set
     */
    public void setTableRouter(TableRouter tableRouter) {
        this.tableRouter = tableRouter;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode() */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((schemaConfig == null) ? 0 : schemaConfig.hashCode());
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
        TableConfig other = (TableConfig) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (schemaConfig == null) {
            if (other.schemaConfig != null)
                return false;
        } else if (!schemaConfig.equals(other.schemaConfig))
            return false;
        return true;
    }


}