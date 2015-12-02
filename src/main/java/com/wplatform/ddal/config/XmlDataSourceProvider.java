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
// Created on 2014年7月30日
// $Id$

package com.wplatform.ddal.config;

import javax.sql.DataSource;

import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;

import java.util.Map;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class XmlDataSourceProvider implements DataSourceProvider {

    private final Map<String, DataSource> dataNodes = New.hashMap();

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


    @Override
    public DataSource lookup(String uid) {
        if (StringUtils.isNullOrEmpty(uid)) {
            throw new DataSourceException("DataSource id be not null.");
        }
        DataSource result = dataNodes.get(uid);
        return result;
    }

}
