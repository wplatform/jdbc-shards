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
package com.wplatform.ddal.engine;

import com.wplatform.ddal.config.Configuration;
import com.wplatform.ddal.config.parser.XmlConfigParser;
import com.wplatform.ddal.dbobject.User;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.util.Utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class Engine implements SessionFactory {

    private static final Engine INSTANCE = new Engine();
    private static final HashMap<String, Database> DATABASES = New.hashMap();

    private Engine() {
        Runtime.getRuntime().addShutdownHook(new Closer());
    }

    public static Engine getInstance() {
        return INSTANCE;
    }

    private static String removeProperty(Properties info, String key) {
        return (String) info.remove(key);
    }

    @Override
    public SessionInterface createSession(String url, Properties ci) throws SQLException {
        if (StringUtils.isNullOrEmpty(url)) {
            throw new IllegalArgumentException();
        }
        return INSTANCE.openSession(url, ci);
    }

    private synchronized Session openSession(String url, Properties info) {
        String userName = removeProperty(info, "user");
        String password = removeProperty(info, "password");
        if (userName == null) {
            userName = Database.SYSTEM_USER_NAME;
        }
        Database database = DATABASES.get(url);
        if (database == null) {
            Properties urlProps = parseURL(url, info);
            String configLocation = removeProperty(urlProps, "configLocation");
            if (StringUtils.isNullOrEmpty(configLocation)) {
                throw new IllegalArgumentException("config file must not be null");
            }
            String resource = removeProperty(urlProps, "resource");
            InputStream source;
            if ("classpath".equals(resource)) {
                source = Utils.getResourceAsStream(configLocation);
                if (source == null) {
                    throw new IllegalArgumentException("Can't load " + configLocation + " from classpath.");
                }
            } else {
                try {
                    source = new BufferedInputStream(new FileInputStream(configLocation));
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException("Can't load " + configLocation + " from filesystem.");
                }
            }
            removeProperty(urlProps, "remote");
            removeProperty(urlProps, "ssl");
            XmlConfigParser parser = new XmlConfigParser(source);
            Configuration configuration = parser.parse();
            HashMap<String, Object> settings = configuration.getSettings();
            Enumeration<?> enumeration = urlProps.propertyNames();
            while (enumeration.hasMoreElements()) {
                String key = (String) enumeration.nextElement();
                String value = urlProps.getProperty(key);
                Object old = settings.get(key);
                if (old != null && !old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                } else {
                    settings.put(key, value);
                }
            }
            database = new Database(configuration);
            User user = database.findUser(userName);
            if (user == null) {
                // users is the last thing we add, so if no user is around,
                // the database is new (or not initialized correctly)
                user = new User(database, database.allocateObjectId(), userName);
                user.setAdmin(true);
                user.setPassword(password);
                database.addDatabaseObject(user);
            }
            DATABASES.put(url, database);
        }
        User userObject = database.getUser(userName);
        Session session = database.createSession(userObject);
        return session;

    }

    public Properties parseURL(String url, Properties defaults) {
        Properties urlProps = (defaults != null) ? new Properties(defaults) : new Properties();
        int idx = url.indexOf(';');
        if (idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx);
            String[] list = StringUtils.arraySplit(settings, ';', false);
            for (String setting : list) {
                if (setting.length() == 0) {
                    continue;
                }
                int equal = setting.indexOf('=');
                if (equal < 0) {
                    throw getFormatException(settings);
                }
                String value = setting.substring(equal + 1);
                String key = setting.substring(0, equal);
                key = StringUtils.toUpperEnglish(key);
                String old = defaults.getProperty(key);
                if (old != null && !old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }
                urlProps.setProperty(key, value);
            }
        }
        url = url.substring(Constants.START_URL.length());
        if (url.startsWith("tcp:")) {
            urlProps.put("remote", true);
            url = url.substring("tcp:".length());
        } else if (url.startsWith("ssl:")) {
            urlProps.put("remote", true);
            urlProps.put("ssl", true);
            url = url.substring("ssl:".length());
        } else if (url.startsWith("file:")) {
            urlProps.put("resource", "file");
            url = url.substring("file:".length());
        } else if (url.startsWith("classpath:")) {
            urlProps.put("resource", "classpath");
            url = url.substring("classpath:".length());
        } else {

        }
        urlProps.put("configLocation", url);
        return urlProps;
    }

    /**
     * Generate an URL format exception.
     *
     * @return the exception
     */
    DbException getFormatException(String url) {
        String format = Constants.URL_FORMAT;
        return DbException.get(ErrorCode.URL_FORMAT_ERROR_2, format, url);
    }

    private static class Closer extends Thread {
        private Closer() {
            super("database-engine-closer");
        }

        @Override
        public void run() {
            synchronized (INSTANCE) {
                for (Database database : DATABASES.values()) {
                    try {
                        database.close();
                    } catch (Exception e) {

                    }
                }
                DATABASES.clear();
            }

        }

    }

}
