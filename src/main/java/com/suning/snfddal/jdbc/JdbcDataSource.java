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
import java.util.Map;

import javax.sql.DataSource;

import com.suning.snfddal.config.Configuration;
import com.suning.snfddal.config.ConfigurationException;
import com.suning.snfddal.config.DataSourceLookup;
import com.suning.snfddal.config.SchemaConfig;
import com.suning.snfddal.config.ShardConfig;
import com.suning.snfddal.config.TableConfig;
import com.suning.snfddal.config.parser.XmlConfigParser;
import com.suning.snfddal.dbobject.User;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.MappedTable;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.SessionInterface;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.StringUtils;
import com.suning.snfddal.util.Utils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class JdbcDataSource implements DataSource {

    private static final String DATABASE_MASTER_USER = "MASTER";

    private PrintWriter logWriter;
    private int loginTimeout;
    private String userName = "";
    private char[] passwordChars = { };
    private String url = "";
    
    private Database database;
    private String configLocation;
    private DataSourceLookup dataSourceLookup;
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
        return getConnection(DATABASE_MASTER_USER, null);
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
     * @return the dataSourceLookup
     */
    public DataSourceLookup getDataSourceLookup() {
        return dataSourceLookup;
    }

    /**
     * @param dataSourceLookup the dataSourceLookup to set
     */
    public void setDataSourceLookup(DataSourceLookup dataSourceLookup) {
        this.dataSourceLookup = dataSourceLookup;
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
        SchemaConfig dsConfig = configuration.getSchemaConfig();
        this.database = new Database();
        Map<String, ShardConfig> shardMapping = configuration.getCluster();
        for (ShardConfig value : shardMapping.values()) {
            String description = value.getDescription();
            //TODO 处理数据源组
            DataSource dataSource = configuration.getDataNodes().get(description);
            if(dataSource == null) {
                throw new ConfigurationException("Can' find data source: " + description);
            }
            database.addDataNode(value.getName(), dataSource);
        }
        
        Schema schema = database.findSchema(dsConfig.getName());
        String userName = DATABASE_MASTER_USER;
        if (schema == null) {
            User user = database.findUser(userName);
            if (database.findUser(userName) == null) {
                // users is the last thing we add, so if no user is around,
                // the database is new (or not initialized correctly)
                user = new User(database, database.allocateObjectId(), userName);
                user.setAdmin(true);
                user.setUserPasswordHash(new byte[0]);
                database.addDatabaseObject(user);
            }
            schema = new Schema(database, database.allocateObjectId(), dsConfig.getName(), user, true);
            database.addDatabaseObject(schema);
        }
        for (TableConfig tbConfig : dsConfig.getTables()) {
            String metadata = tbConfig.getMetadata();
            String metaNode = metadata;
            String originalTable = tbConfig.getName();
            int dotPost = metadata.indexOf('.');
            if(dotPost != -1) {
                metaNode = metadata.substring(0, dotPost);
                originalTable = metadata.substring(dotPost + 1);
            }
            MappedTable tableObject = schema.createMappedTable(database.allocateObjectId(), tbConfig.getName(),
                    metaNode, null, originalTable, false, false);
            tableObject.setTableRouter(tbConfig.getTableRouter());
            database.addSchemaObject(tableObject);
        }
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
