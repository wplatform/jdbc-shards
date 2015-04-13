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
package com.suning.snfddal.jdbc;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.suning.snfddal.command.dml.SetTypes;
import com.suning.snfddal.config.Configuration;
import com.suning.snfddal.config.DataSourceProvider;
import com.suning.snfddal.config.parser.XmlConfigParser;
import com.suning.snfddal.dbobject.User;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.SessionInterface;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.util.Utils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class JdbcDataSource implements DataSource {
    
    private PrintWriter logWriter;
    private int loginTimeout;
    private String userName = "";
    private char[] passwordChars = { };
    private String url = "";
    
    private Properties prop = new Properties();
    private Database database;
    private String configLocation;
    private DataSourceProvider dataSourceProvider;
    private boolean inited = false;
    
    
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
        return getConnection(Database.SYSTEM_USER_NAME, null);
    }

    /**
     * Open a new connection using the current URL and the specified user name
     * and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    @Override
    public Connection getConnection(String user, String password) throws SQLException {
        User userObject = database.getUser(user);
        SessionInterface session = database.createSession(userObject);
        JdbcConnection conn = new JdbcConnection(session, user, this.configLocation);
        return conn;

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
        this.url = url;
    }

    /**
     * Get the current URL.
     * This method does the same as getURL, but this methods signature conforms
     * the JavaBean naming convention.
     *
     * @return the URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Set the current URL.
     * This method does the same as setURL, but this methods signature conforms
     * the JavaBean naming convention.
     *
     * @param url the new URL
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Set the current password.
     *
     * @param password the new password.
     */
    public void setPassword(String password) {
        this.passwordChars = convertToCharArray(password);
    }
    /**
     * Get the current password.
     *
     * @return the password
     */
    public String getPassword() {
        return convertToString(passwordChars);
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
    public void setUserName(String user) {
        this.userName = user;
    }
    /**
     * Set the current password in the form of a char array.
     *
     * @param password the new password in the form of a char array.
     */
    public void setPasswordChars(char[] password) {
        this.passwordChars = password;
    }

    private static char[] convertToCharArray(String s) {
        return s == null ? null : s.toCharArray();
    }

    private static String convertToString(char[] a) {
        return a == null ? null : new String(a);
    }

    @Override
    public String toString() {
        return "DispatcherDataSource [configLocation=" + configLocation + "]";
    }

    /**
     * @return the configLocation
     */
    public String getConfigLocation() {
        return configLocation;
    }

    /**
     * @param configLocation the configLocation to set
     */
    public void setConfigLocation(String configLocation) {
        this.configLocation = configLocation;
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
        this.dataSourceProvider = dataSourceProvider;
    }

    /**
     * @param sqlMode the sqlMode to set
     */
    public void setSqlMode(String sqlMode) {
        String varName = SetTypes.getTypeName(SetTypes.MODE);
        prop.setProperty(varName, sqlMode);
    }
    
    /**
     * @param outputLogLevel the outputLogLevel to set
     */
    public void setOutputLogLevel(String outputLogLevel) {
        String varName = SetTypes.getTypeName(SetTypes.TRACE_LEVEL_SYSTEM_OUT);
        prop.setProperty(varName, outputLogLevel);
    }
    
    /**
     * @param fileLogLevel the fileLogLevel to set
     */
    public void setFileLogLevel(String fileLogLevel) {
        String varName = SetTypes.getTypeName(SetTypes.TRACE_LEVEL_FILE);
        prop.setProperty(varName, fileLogLevel);
    }

    public synchronized void init() {
        if (inited) {
            return;
        }
        if (StringUtils.isNullOrEmpty(this.configLocation)) {
            throw new IllegalArgumentException("Property configLocation must not be null");
        }
        InputStream source = Utils.getResourceAsStream(configLocation);
        if (source == null) {
            throw new IllegalArgumentException("Can't load the configLocation resource " + configLocation);
        }
        XmlConfigParser parser = new XmlConfigParser(source);
        Configuration configuration = parser.parse();
        configuration.getSettings().putAll(prop);
        this.database = new Database(configuration);
        inited = true;
    }

    public synchronized void close() {
        if(database == null) {
            return;
        }
        database.close();
        database = null;
    }
}
