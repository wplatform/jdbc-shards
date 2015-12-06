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
package com.wplatform.ddal.jdbc;

import com.wplatform.ddal.command.dml.SetTypes;
import com.wplatform.ddal.config.DataSourceProvider;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.StringUtils;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class JdbcDataSource implements DataSource {

    private PrintWriter logWriter;
    private int loginTimeout;
    private String userName;
    private String password;
    private String url;

    private String dbType;
    private int defaultQueryTimeout = -1;

    private DataSourceProvider dataSourceProvider;

    private Properties properties = new Properties();

    private volatile boolean inited = false;

    /**
     * The public constructor.
     */
    public JdbcDataSource() {

    }

    /**
     * Get the login timeout in seconds, 0 meaning no timeout.
     *
     * @return the timeout in seconds
     */
    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    /**
     * Set the login timeout in seconds, 0 meaning no timeout. The default value
     * is 0. This value is ignored by this database.
     *
     * @param timeout the timeout in seconds
     */
    @Override
    public void setLoginTimeout(int timeout) {
        this.loginTimeout = timeout;
    }

    /**
     * Get the current log writer for this object.
     *
     * @return the log writer
     */
    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * Set the current log writer for this object. This value is ignored by this
     * database.
     *
     * @param out the log writer
     */
    @Override
    public void setLogWriter(PrintWriter out) {
        logWriter = out;
    }

    /**
     * Open a new connection using the current URL, user name and password.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getJdbcConnection(this.userName, this.password);
    }

    /**
     * Open a new connection using the current URL and the specified user name
     * and password.
     *
     * @param user     the user name
     * @param password the password
     * @return the connection
     */
    @Override
    public Connection getConnection(String user, String password) throws SQLException {
        return getJdbcConnection(user, password);
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw DbException.getInvalidValueException("iface", iface);
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * [Not supported]Java 1.7
     */
    public Logger getParentLogger() {
        return null;
    }

    /**
     * Get the current URL.
     *
     * @return the URL
     */
    public String getURL() {
        return url;
    }

    /**
     * Set the current URL.
     *
     * @param url the new URL
     */
    public void setURL(String url) {
        checkState();
        this.url = url;
    }

    /**
     * Get the current URL. This method does the same as getURL, but this
     * methods signature conforms the JavaBean naming convention.
     *
     * @return the URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Set the current URL. This method does the same as setURL, but this
     * methods signature conforms the JavaBean naming convention.
     *
     * @param url the new URL
     */
    public void setUrl(String url) {
        checkState();
        this.url = url;
    }

    /**
     * Get the current password.
     *
     * @return the password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Set the current password.
     *
     * @param password the new password.
     */
    public void setPassword(String password) {
        checkState();
        this.password = password;
    }

    /**
     * Get the current user name.
     *
     * @return the user name
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Set the current user name.
     *
     * @param user the new user name
     */
    public void setUserName(String userName) {
        checkState();
        this.userName = userName;
    }

    /**
     * @return the dataSourceProvider
     */
    public DataSourceProvider getDataSourceProvider() {
        return dataSourceProvider;
    }

    /**
     * @param dataSourceProvider the dataSourceProvider to set
     */
    public void setDataSourceProvider(DataSourceProvider dataSourceProvider) {
        checkState();
        this.dataSourceProvider = dataSourceProvider;
    }

    /**
     * @return the dbType
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * @param dbType the dbType to set
     */
    public void setDbType(String dbType) {
        checkState();
        this.dbType = dbType;
    }


    /**
     * @return the defaultQueryTimeout
     */
    public int getDefaultQueryTimeout() {
        return defaultQueryTimeout;
    }

    /**
     * @param defaultQueryTimeout the defaultQueryTimeout to set
     */
    public void setDefaultQueryTimeout(int defaultQueryTimeout) {
        checkState();
        this.defaultQueryTimeout = defaultQueryTimeout;
    }

    private JdbcConnection getJdbcConnection(String user, String password) throws SQLException {
        if (!inited) {
            init();
        }
        if (!StringUtils.isNullOrEmpty(user)) {
            properties.setProperty("user", user);
        }
        if (!StringUtils.isNullOrEmpty(password)) {
            properties.setProperty("password", password);
        }
        Connection conn = JdbcDriver.load().connect(url, properties);
        if (conn == null) {
            throw new SQLException("No suitable driver found for " + url, "08001", 8001);
        } else if (!(conn instanceof JdbcConnection)) {
            throw new SQLException("Connecting with old version is not supported: " + url, "08001", 8001);
        }
        return (JdbcConnection) conn;
    }


    public void init() {
        if (inited) {
            return;
        }
        properties = new Properties();
        properties.put(SetTypes.getTypeName(SetTypes.MODE), this.dbType);
        if (this.dataSourceProvider != null) {
            properties.put("dataSourceProvider", this.dataSourceProvider);
        }
        inited = true;
    }

    private void checkState() {
        if (inited) {
            throw new IllegalStateException("");
        }
    }

}
