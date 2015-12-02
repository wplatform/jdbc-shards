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
// Created on 2015年1月15日
// $Id$

package com.wplatform.ddal.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ConnectionTestCase extends BaseTestCase {
    
    public void testConnection(String sql, List<Object> params) {
        Connection conn = null;
        boolean succee = true;
        try {
            conn = dataSource.getConnection();
            conn.getTransactionIsolation();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.isReadOnly();
            conn.setReadOnly(true);
            conn.isReadOnly();
            conn.setAutoCommit(false);            
            conn.rollback();
            conn.commit();
        } catch (SQLException e) {
            succee = false;
            e.printStackTrace();
        } finally {
            close(conn, null, null);
        }
        Assert.assertEquals(true, succee);
    }
    
    public void query_Sql(String sql, List<Object> params) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        boolean succee = true;
        try {
            conn = dataSource.getConnection();
            conn.getTransactionIsolation();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.isReadOnly();
            conn.setReadOnly(true);
            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sql);
            if(params != null) {
                int index = 1;
                for (Object object : params) {
                    statement.setObject(index++, object);
                }
            }
            resultSet = statement.executeQuery();
            conn.commit();
        } catch (SQLException e) {
            succee = false;
            try {
                conn.rollback();
            } catch (Exception e1) {
                
            }
            e.printStackTrace();
        } finally {
            close(conn, statement, resultSet);
        }
        Assert.assertEquals(true, succee);
    }
    
    


    @Test
    public void testQuery_AllColumn(){
        String sql = "SELECT * FROM t_student where f_student_id > 3 or F_SCHOOL_ID < 100";
        this.query_Sql(sql, null);
    }
    
  
}
