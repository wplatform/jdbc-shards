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
// Created on 2014年3月25日
// $Id$

package com.wplatform.ddal.config;

import com.wplatform.ddal.command.dml.SetTypes;
import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.route.algorithm.Partitioner;
import com.wplatform.ddal.route.rule.TableRouter;
import com.wplatform.ddal.util.New;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class Configuration {

    private final Map<String, ShardConfig> cluster = New.hashMap();
    private final Map<String, Partitioner> partitioners = New.hashMap();
    private final Map<String, TableRouter> tableRouterTemplate = New.hashMap();
    private HashMap<String, Object> prop = New.hashMap();
    private SchemaConfig schemaConfig = new SchemaConfig();

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
     * @return
     */
    public HashMap<String, Object> getSettings() {
        return this.prop;
    }

    /**
     * Get the property keys.
     *
     * @return the property keys
     */
    String[] getKeys() {
        String[] keys = new String[prop.size()];
        prop.keySet().toArray(keys);
        return keys;
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @return the value as a String
     */
    public String getProperty(String key) {
        Object value = prop.get(key);
        if (value == null || !(value instanceof String)) {
            return null;
        }
        return value.toString();
    }

    /**
     * Get the value of the given property.
     *
     * @param key          the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public int getProperty(String key, int defaultValue) {
        String s = getProperty(key);
        return s == null ? defaultValue : Integer.parseInt(s);
    }

    /**
     * Get the value of the given property.
     *
     * @param key          the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public String getProperty(String key, String defaultValue) {
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as a String
     */
    public String getProperty(int setting, String defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    public int getIntProperty(int setting, int defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key, null);
        try {
            return s == null ? defaultValue : Integer.decode(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    public boolean getBooleanProperty(int setting, boolean defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key, null);
        try {
            return s == null ? defaultValue : Boolean.parseBoolean(s);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    public <T> T getObject(int setting) {
        String key = SetTypes.getTypeName(setting);
        return getObject(key);
    }

    /**
     * Get the value of the given property.
     *
     * @param setting      the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    @SuppressWarnings("unchecked")
    public <T> T getObject(String key) {
        T result = (T) prop.get(key);
        return result;
    }

    /**
     * Remove a boolean property if it is set and return the value.
     *
     * @param key          the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean removeProperty(String key, boolean defaultValue) {
        String x = removeProperty(key, null);
        return x == null ? defaultValue : Boolean.parseBoolean(x);
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setProperty(int setting, String value) {
        String key = SetTypes.getTypeName(setting);
        if (SysProperties.CHECK && key == null) {
            DbException.throwInternalError(key);
        }
        // value is null if the value is an object
        if (value != null) {
            prop.put(key, value);
        }
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setProperty(int setting, int value) {
        setProperty(setting, String.valueOf(value));
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setProperty(int setting, boolean value) {
        setProperty(setting, String.valueOf(value));
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setProperty(String key, String value) {
        // value is null if the value is an object
        if (value != null) {
            prop.put(key, value);
        }
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setObject(String key, Object value) {
        // value is null if the value is an object
        if (value != null) {
            prop.put(key, value);
        }
    }

    /**
     * Overwrite a property.
     *
     * @param key   the property name
     * @param value the value
     */
    public void setObject(int setting, Object value) {
        String key = SetTypes.getTypeName(setting);
        setObject(key, value);
    }

    /**
     * Remove a String property if it is set and return the value.
     *
     * @param key          the property name
     * @param defaultValue the default value
     * @return the value
     */
    String removeProperty(String key, String defaultValue) {
        Object x = prop.remove(key);
        return x == null ? defaultValue : x.toString();
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
            throw new IllegalArgumentException("Duplicate shard name " + name);
        }
        cluster.put(name, shard);
    }

    public void addTableRouterTemplate(String id, TableRouter template) {
        if (tableRouterTemplate.containsKey(id)) {
            throw new IllegalArgumentException("Duplicate table router id " + id);
        }
        tableRouterTemplate.put(id, template);
    }

    /**
     * @return the tableRules
     */
    public Map<String, TableRouter> getTableRouterTemplates() {
        return tableRouterTemplate;
    }

    /**
     * @return the partitioner
     */
    public Partitioner getPartitioner(String name) {
        Partitioner partitioner = partitioners.get(name);
        if (partitioner == null) {
            throw new IllegalArgumentException("Duplicate partitioner name " + name);
        }
        return partitioner;
    }

    public void addPartitioner(String name, Partitioner partitioner) {
        if (partitioners.containsKey(name)) {
            throw new IllegalArgumentException("Duplicate partitioner name " + name);
        }
        partitioners.put(name, partitioner);
    }

}
