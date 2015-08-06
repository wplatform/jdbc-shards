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
package com.wplatform.ddal.test;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.After;

import com.wplatform.ddal.jdbc.JdbcDataSource;
import com.wplatform.ddal.util.Utils;

public abstract class BaseSampleCase {

    protected DataSource dataSource;

    public BaseSampleCase() {
        try {
            String configLocation = "/config/ddal-config.xml";
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setConfigLocation(configLocation);
            dataSource.setStdoutLevel("DEBUG");
            dataSource.setFileLevel("DEBUG");
            dataSource.setLogFileName("database.log");
            dataSource.init();
            this.dataSource = dataSource;
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @After
    public void disorty() {
        try {
            ((JdbcDataSource) dataSource).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close(Connection connection, Statement statement, ResultSet rs) {
        if (rs != null) {
            try {
                if (!rs.isClosed())
                    rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (statement != null) {
            try {
                if (!statement.isClosed())
                    statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                if (!connection.isClosed())
                    connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public long getUUID() {
        UUID uuid = UUID.randomUUID();
        long murmurhash2_64 = Utils.murmurhash2_64(uuid.toString());
        murmurhash2_64 = Math.abs(murmurhash2_64);
        return murmurhash2_64;
    }

    public int nextOrderSeqVaule(Connection conn) throws SQLException {
        PreparedStatement ptmt = null;
        ResultSet rs = null;
        String sql = "SELECT nextval('order_seq')";
        try {
            conn = this.dataSource.getConnection();
            ptmt = conn.prepareStatement(sql);
            rs = ptmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw e;
        } finally {
            close(null, ptmt, rs);
        }
    }

    // public static void main(String[] args) throws ClassNotFoundException,
    // SQLException {
    // Class.forName("com.ibm.db2.jcc.DB2Driver");
    // Connection conn =
    // DriverManager.getConnection("jdbc:db2://10.19.250.15:60000/simpledb:currentSchema=DDAL;","db2inst1","FNU9r@bdq");
    // PreparedStatement ps =null;
    // ResultSet rSet = null;
    // try {
    // ps = conn.prepareStatement("select name from test where id=1");
    // rSet = ps.executeQuery();
    // while(rSet.next()){
    // System.out.println(rSet.getString(1)+"***********");
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // } finally{
    // }
    // }
    //

}
