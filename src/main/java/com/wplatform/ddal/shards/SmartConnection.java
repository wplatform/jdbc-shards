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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class SmartConnection extends SmartSupport implements InvocationHandler {

    private Connection connection;

    /**
     * @param database
     * @param dataSource
     * @param traceable
     */
    protected SmartConnection(DataSourceRepository database, SmartDataSource dataSource, Connection connection) {
        super(database, dataSource);
        this.connection = connection;
    }

    /**
     * Creates a exception trace version of a connection
     *
     * @param conn - the original connection
     * @return - the connection with exception trace
     */
    public static Connection newInstance(DataSourceRepository database, SmartDataSource dataSource, Connection connection) {
        InvocationHandler handler = new SmartConnection(database, dataSource, connection);
        ClassLoader cl = Connection.class.getClassLoader();
        return (Connection) Proxy.newProxyInstance(cl, new Class[]{Connection.class}, handler);
    }

    public Object invoke(Object proxy, Method method, Object[] params) throws Throwable {
        try {
            if ("prepareStatement".equals(method.getName())) {
                if (isDebugEnabled()) {
                    debug("==>  Preparing: " + removeBreakingWhitespace((String) params[0]));
                }
                PreparedStatement stmt = (PreparedStatement) method.invoke(connection, params);
                stmt = SmartPreparedStatement.newInstance(this, stmt);
                return stmt;
            } else if ("prepareCall".equals(method.getName())) {
                if (isDebugEnabled()) {
                    debug("==>  Preparing: " + removeBreakingWhitespace((String) params[0]));
                }
                PreparedStatement stmt = (PreparedStatement) method.invoke(connection, params);
                stmt = SmartPreparedStatement.newInstance(this, stmt);
                return stmt;
            } else if ("createStatement".equals(method.getName())) {
                Statement stmt = (Statement) method.invoke(connection, params);
                stmt = SmartStatement.newInstance(this, stmt);
                return stmt;
            } else if ("close".equals(method.getName())) {
                if (isDebugEnabled()) {
                    debug("Connection Closed");
                }
                return method.invoke(connection, params);
            } else {
                return method.invoke(connection, params);
            }
        } catch (Throwable t) {
            t = unwrapThrowable(t);
            handleException(t);
            throw t;
        }
    }

    /**
     * return the wrapped connection
     *
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
    }

}
