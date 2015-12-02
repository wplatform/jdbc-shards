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
package com.wplatform.ddal.shards;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SmartSupport {

    protected final DataSourceRepository database;
    protected final SmartDataSource dataSource;
    protected final Trace trace;

    /**
     * @param database
     * @param dataSource
     * @param connection
     * @param trace
     * @throws SQLException
     */
    protected SmartSupport(DataSourceRepository database, SmartDataSource dataSource) {
        this.database = database;
        this.dataSource = dataSource;
        this.trace = database.getTrace();
    }


    protected boolean isDebugEnabled() {
        return trace.isDebugEnabled();
    }

    protected void debug(String text) {
        if (trace.isDebugEnabled()) {
            trace.debug(dataSource.toString() + text);
        }
    }
    
    
    protected Connection applyConnection(boolean readOnly) throws SQLException {
        return applyConnection(readOnly, null, null);
    }
    
    protected Connection applyConnection(boolean readOnly,String username, String password) throws SQLException {
        List<DataSourceMarker> tryList = New.arrayList();
        DataSourceMarker selected = dataSource.doRoute(readOnly);
        while(selected != null) {
            try {
                tryList.add(selected);
                return (username != null) ? database.getConnection(selected, username, password)
                        : database.getConnection(selected); 
            }catch(SQLException e) {
                selected = dataSource.doRoute(readOnly, tryList);
            }
        }
        throw new SQLException("No avaliable datasource in shard " + dataSource);

    }

}
