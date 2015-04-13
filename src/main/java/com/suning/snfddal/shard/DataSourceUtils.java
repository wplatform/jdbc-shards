/*
 * Copyright 2015 suning.com Holding Ltd.
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
// Created on 2015年4月13日
// $Id$

package com.suning.snfddal.shard;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DataSourceUtils {
    public static Connection getConnection(UidDataSource dataSource) throws SQLException {
        try {
            return doGetConnection(dataSource);
        } catch (SQLException ex) {
            throw ex;
        }
    }

    public static Connection doGetConnection(UidDataSource dataSource) throws SQLException {
        if (dataSource == null) {
            throw new IllegalArgumentException("No DataSource specified");
        }
        String uid = dataSource.getUid();
        if (!dataSource.isWritable()) {
            // logger.debug("Fetching JDBC Connection from DataSource");
            return dataSource.getConnection();
        }
        ConnectionHolder conHolder = Synchronization.getResource(dataSource);
        if (conHolder != null) {
            Connection conn = conHolder.getConnection(uid);
            if (conn == null) {
                //logger.debug("Fetching resumed JDBC Connection from DataSource");
                conHolder.enlistConnection(uid, dataSource.getConnection());
            }
            return conHolder.getConnection(uid);
        }
        return dataSource.getConnection();
    }
}
