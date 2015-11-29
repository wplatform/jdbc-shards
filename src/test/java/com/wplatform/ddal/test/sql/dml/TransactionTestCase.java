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
package com.wplatform.ddal.test.sql.dml;

import java.sql.Connection;

import org.junit.Test;

import com.wplatform.ddal.jdbc.JdbcConnection;
import com.wplatform.ddal.test.BaseTestCase;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class TransactionTestCase extends BaseTestCase {


    @Test
    public void test() throws Exception {
        //setQueryTimeout();
        //testSetAutoCommit();
        testSetReadOnly();
        testsetIsolationLevel();
    }
    
    
    public void setQueryTimeout() throws Exception {
        JdbcConnection conn = null;
        try {
            conn = (JdbcConnection) getConnection();
            conn.setQueryTimeout(10);
        } finally {
            close(conn, null, null);
        }
    }
    
    public void testSetAutoCommit() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            conn.setAutoCommit(true);
        } finally {
            close(conn, null, null);
        }
    }
    

    
    public void testSetReadOnly() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setReadOnly(false);
            conn.setReadOnly(true);
        } finally {
            close(conn, null, null);
        }
    }
    
    public void testsetIsolationLevel() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        }catch(Exception e) {
            
        }finally {
            close(conn, null, null);
        }
    }
    
    @Test
    public void performance() throws Exception {
        Connection conn = null;
        try {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                conn = getConnection();
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                close(conn, null, null);
            }
            long end = System.currentTimeMillis();
            System.out.println("cost " + (end - start) + "ms.");
            /*
            start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                conn = getConnection();
                conn.getTransactionIsolation();
                close(conn, null, null);
            }
            end = System.currentTimeMillis();
            System.out.println("cost " + (end - start) + "ms.");
            */

        }catch(Exception e) {
            
        }finally {
            close(conn, null, null);
        }
    }
    
    

}
