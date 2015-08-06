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

package com.wplatform.ddal.tx;

import java.sql.Connection;
import java.util.Map;

import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ConnectionHolder {

    public static final String SAVEPOINT_NAME_PREFIX = "SAVEPOINT_";


    private Map<String, Connection> resources = New.hashMap();

    public Connection getConnection(String uid) {
        return resources.get(uid);
    }

    public void enlistConnection(String uid, Connection connection) {
        resources.put(uid, connection);
    }

}
