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
// Created on 2014年11月17日
// $Id$

package com.wplatform.ddal.test.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class QueryTestCase extends BaseTestCase{
    
    public void query_Sql(String sql, List<Object> params) {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        boolean succee = true;
        try {
            conn = dataSource.getConnection();
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
    public void test_inner_join_conndiditon(){
        String sql = "SELECT * FROM orders a inner join order_items b on a.order_id=b.order_id inner join order_status c on b.order_id = c.order_id where customer_id = 1";
        this.query_Sql(sql, null);
    }
    
  
}
