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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class SmartConnection extends SmartSupport implements InvocationHandler {

    private String username;
    private String password;
    private Boolean readOnly = Boolean.FALSE;
    private Integer transactionIsolation;
    private Boolean autoCommit;
    private boolean closed = false;

    private Connection target;

    /**
     * @param database
     * @param dataSource
     * @param traceable
     * @throws SQLException
     */
    protected SmartConnection(DataSourceRepository database, SmartDataSource dataSource) {
        super(database, dataSource);
    }

    /**
     * @param database
     * @param dataSource
     * @param username
     * @param password
     * @throws SQLException
     */
    protected SmartConnection(DataSourceRepository database, SmartDataSource dataSource, String username,
            String password) {
        this(database, dataSource);
        this.username = username;
        this.password = password;
    }

    /**
     * Creates a exception trace version of a connection
     *
     * @param conn - the original connection
     * @return - the connection with exception trace
     * @throws SQLException
     */
    public static Connection newInstance(DataSourceRepository database, SmartDataSource dataSource) {
        InvocationHandler handler = new SmartConnection(database, dataSource);
        ClassLoader cl = Connection.class.getClassLoader();
        return (Connection) Proxy.newProxyInstance(cl, new Class[] { Connection.class }, handler);
    }

    /**
     * Creates a exception trace version of a connection
     *
     * @param conn - the original connection
     * @return - the connection with exception trace
     * @throws SQLException
     */
    public static Connection newInstance(DataSourceRepository database, SmartDataSource dataSource, String username,
            String password) {
        InvocationHandler handler = new SmartConnection(database, dataSource, username, password);
        ClassLoader cl = Connection.class.getClassLoader();
        return (Connection) Proxy.newProxyInstance(cl, new Class[] { Connection.class }, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Invocation on ConnectionProxy interface coming in...

        if (method.getName().equals("equals")) {
            // We must avoid fetching a target Connection for "equals".
            // Only consider equal when proxies are identical.
            return (proxy == args[0]);
        } else if (method.getName().equals("hashCode")) {
            // We must avoid fetching a target Connection for "hashCode",
            // and we must return the same hash code even when the target
            // Connection has been fetched: use hashCode of Connection proxy.
            return System.identityHashCode(proxy);
        } else if (method.getName().equals("unwrap")) {
            if (((Class<?>) args[0]).isInstance(proxy)) {
                return proxy;
            }
        } else if (method.getName().equals("isWrapperFor")) {
            if (((Class<?>) args[0]).isInstance(proxy)) {
                return true;
            }
        }

        if (!hasTargetConnection()) {
            // No physical target Connection kept yet ->
            // resolve transaction demarcation methods without fetching
            // a physical JDBC Connection until absolutely necessary.

            if (method.getName().equals("toString")) {
                return "Routing Connection proxy for RoutingDataSource [" + dataSource + "]";
            } else if (method.getName().equals("isReadOnly")) {
                return this.readOnly;
            } else if (method.getName().equals("setReadOnly")) {
                this.readOnly = (Boolean) args[0];
                return null;
            } else if (method.getName().equals("getTransactionIsolation")) {
                if (this.transactionIsolation != null) {
                    return this.transactionIsolation;
                }
                // Else fetch actual Connection and check there,
                // because we didn't have a default specified.
            } else if (method.getName().equals("setTransactionIsolation")) {
                this.transactionIsolation = (Integer) args[0];
                return null;
            } else if (method.getName().equals("getAutoCommit")) {
                if (this.autoCommit != null) {
                    return this.autoCommit;
                }
                // Else fetch actual Connection and check there,
                // because we didn't have a default specified.
            } else if (method.getName().equals("setAutoCommit")) {
                this.autoCommit = (Boolean) args[0];
                return null;
            } else if (method.getName().equals("commit")) {
                // Ignore: no statements created yet.
                return null;
            } else if (method.getName().equals("rollback")) {
                // Ignore: no statements created yet.
                return null;
            } else if (method.getName().equals("getWarnings")) {
                return null;
            } else if (method.getName().equals("clearWarnings")) {
                return null;
            } else if (method.getName().equals("close")) {
                // Ignore: no target connection yet.
                this.closed = true;
                return null;
            } else if (method.getName().equals("isClosed")) {
                return this.closed;
            } else if (this.closed) {
                // Connection proxy closed, without ever having fetched a
                // physical JDBC Connection: throw corresponding SQLException.
                throw new SQLException("Illegal operation: connection is closed");
            }
        }

        // Target Connection already fetched,
        // or target Connection necessary for current operation ->
        // invoke method on target connection.
        try {
            return method.invoke(getTargetConnection(method), args);
        } catch (InvocationTargetException ex) {
            throw ex.getTargetException();
        }
    }

    /**
     * Return whether the proxy currently holds a target Connection.
     */
    private boolean hasTargetConnection() {
        return (this.target != null);
    }

    /**
     * Return the target Connection, fetching it and initializing it if
     * necessary.
     */
    private Connection getTargetConnection(Method operation) throws SQLException {
        if (this.target == null) {
            // No target Connection held -> fetch one.
            debug("Connecting to database for operation '" + operation.getName() + "'");
            // Fetch physical Connection from DataSource.
            this.target = (this.username != null) ? applyConnection(this.readOnly, this.username, this.password)
                    : applyConnection(this.readOnly);

            // Apply kept transaction settings, if any.
            if (this.readOnly) {
                try {
                    this.target.setReadOnly(this.readOnly);
                } catch (Exception ex) {
                    // "read-only not supported" -> ignore, it's just a hint
                    // anyway
                    if (trace.isDebugEnabled()) {
                        trace.debug(ex, "Could not set JDBC Connection read-only");
                    }
                }
            }
            if (this.transactionIsolation != null) {
                this.target.setTransactionIsolation(this.transactionIsolation);
            }
            if (this.autoCommit != null) {
                this.target.setAutoCommit(this.autoCommit);
            }
        } else {
            // Target Connection already held -> return it.
            debug("Using existing database connection for operation '" + operation.getName() + "'");
        }

        return this.target;
    }

}
