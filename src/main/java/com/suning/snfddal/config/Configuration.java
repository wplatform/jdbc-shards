/*
 * Copyright 2014 suning.com Holding Ltd.
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
// Created on 2014年3月25日
// $Id$

package com.suning.snfddal.config;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import com.suning.snfddal.route.rule.TableRouter;
import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class Configuration {
    
    
    private static Configuration instance = new Configuration();
    
    public static Configuration getInstance() {
        return instance;
    }

    // ddal-config
    private final Properties settings = new Properties();

    private SchemaConfig schemaConfig = new SchemaConfig();

    private final Map<String, ShardConfig> cluster = New.hashMap();

    private final Map<String, DataSource> dataNodes = New.hashMap();
    
    private final Map<String, TableRouter> tableRouters = New.hashMap();

    private final Map<String, Object> ruleAlgorithms = New.hashMap();
    
    private Configuration() {
    }

    public Set<String> getShardNames() {
        return cluster.keySet();
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
     * @return the settings
     */
    public Properties getSettings() {
        return settings;
    }

    /**
     * @param settings the settings to set
     */
    public void addSettings(String name, String value) {
        if (settings.containsKey(name)) {
            throw new ConfigurationException("Duplicate settings name " + name);
        }
        settings.put(name, value);
    }

    /**
     * @return the cluster
     */
    public Map<String, ShardConfig> getCluster() {
        return cluster;
    }

    /**
     * @param cluster the cluster to set
     */
    public void addShard(String name, ShardConfig shard) {
        if (cluster.containsKey(name)) {
            throw new ConfigurationException("Duplicate shard name " + name);
        }
        cluster.put(name, shard);
    }

    /**
     * @return the dataNodes
     */
    public Map<String, DataSource> getDataNodes() {
        return dataNodes;
    }

    public void addDataNode(String id, DataSource dataSource) {
        if (dataNodes.containsKey(id)) {
            throw new ConfigurationException("Duplicate datasource id " + id);
        }
        dataNodes.put(id, dataSource);
    }
    
    public void addTableRouter(String id, TableRouter tableRule) {
        if (tableRouters.containsKey(id)) {
            throw new ConfigurationException("Duplicate table router id " + id);
        }
        tableRouters.put(id, tableRule);
    }
    /**
     * @return the tableRules
     */
    public Map<String, TableRouter> getTableRouters() {
        return tableRouters;
    }

    public void addRuleAlgorithm(String id, TableRouter tableRouter) {
        if (tableRouters.containsKey(id)) {
            throw new ConfigurationException("Duplicate table rule id " + id);
        }
        tableRouters.put(id, tableRouter);
    }

    /**
     * @return the ruleAlgorithms
     */
    public Map<String, Object> getRuleAlgorithms() {
        return ruleAlgorithms;
    }

    public void addRuleAlgorithm(String name, Object ruleAlgorithm) {
        if (ruleAlgorithms.containsKey(name)) {
            throw new ConfigurationException("Duplicate ruleAlgorithm name " + name);
        }
        ruleAlgorithms.put(name, ruleAlgorithm);
    }

    public static class SchemaConfig {
        private String name;
        private String metadata;
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
         * @return the metadata
         */
        public String getMetadata() {
            return metadata;
        }

        /**
         * @param metadata the metadata to set
         */
        public void setMetadata(String metadata) {
            this.metadata = metadata;
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

    public static class ShardConfig {
        private String name;
        private String description;
        private Properties properties;

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
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * @param description the description to set
         */
        public void setDescription(String description) {
            this.description = description;
        }
        /**
         * @return the properties
         */
        public Properties getProperties() {
            return properties;
        }

        /**
         * @param properties the properties to set
         */
        public void setProperties(Properties properties) {
            this.properties = properties;
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
            ShardConfig other = (ShardConfig) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

    }

    public static class TableConfig {
        private SchemaConfig schemaConfig;
        private String name;
        private String metadata;
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
         * @return the metadata
         */
        public String getMetadata() {
            return metadata;
        }

        /**
         * @param metadata the metadata to set
         */
        public void setMetadata(String metadata) {
            this.metadata = metadata;
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

    public static class RuleAlgorithmConfig {
        private String name;
        private String clazz;
        private Properties properties;

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
         * @return the clazz
         */
        public String getClazz() {
            return clazz;
        }

        /**
         * @param clazz the clazz to set
         */
        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        /**
         * @return the properties
         */
        public Properties getProperties() {
            return properties;
        }

        /**
         * @param properties the properties to set
         */
        public void setProperties(Properties properties) {
            this.properties = properties;
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
            RuleAlgorithmConfig other = (RuleAlgorithmConfig) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

    }

    public static class DataSourceConfig {
        private String id;
        private String clazz;
        private String jndiName;
        private Properties properties;

        /**
         * @return the id
         */
        public String getId() {
            return id;
        }

        /**
         * @param id the id to set
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * @return the clazz
         */
        public String getClazz() {
            return clazz;
        }

        /**
         * @param clazz the clazz to set
         */
        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        /**
         * @return the jndiName
         */
        public String getJndiName() {
            return jndiName;
        }

        /**
         * @param jndiName the jndiName to set
         */
        public void setJndiName(String jndiName) {
            this.jndiName = jndiName;
        }

        /**
         * @return the properties
         */
        public Properties getProperties() {
            return properties;
        }

        /**
         * @param properties the properties to set
         */
        public void setProperties(Properties properties) {
            this.properties = properties;
        }

        public boolean isJndiDataSource() {
            return jndiName != null;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode() */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            return result;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#equals(java.lang.Object) */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DataSourceConfig other = (DataSourceConfig) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            return true;
        }
    }
}
