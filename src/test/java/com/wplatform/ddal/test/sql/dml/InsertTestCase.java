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
// Created on 2015年1月28日
// $Id$

package com.wplatform.ddal.test.sql.dml;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;

import com.wplatform.ddal.test.BaseTestCase;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class InsertTestCase extends BaseTestCase {

    
    public void execute(String sql, List<Object> params) {
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
            int row = statement.executeUpdate();
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
    public void testInsertNotRouting(){
        String sql = "INSERT INTO t_school(f_id,f_name,f_found_date,f_address,f_gmt)VALUES(?,?,?,?,?)";
        for (int i = 0; i < 1000; i++) {
            List<Object> params = New.arrayList();
            params.add(i);
            params.add("北京大学-" + i);
            params.add("1890-10-10");
            params.add("中国北京");
            params.add(new Date());
            this.execute(sql, params);
        }
    }
    
    
    @Test
    public void testInsertWithRouting(){
        String sql1 = "INSERT INTO t_student (f_student_id,f_student_no,f_name,t_birthday,f_phone,f_sex,f_school_id,f_address,f_gmt)VALUES(?,?,?,?,?,?,?,?,?)";
        String sql2 = "INSERT INTO t_student_course (f_id,f_student_id,t_course_name,f_course_no,t_score,t_learn_year,f_gmt)VALUES(?,?,?,?,?,?,?)";
        int id2 = 0;
        for (int i = 0; i < 1000; i++) {
            List<Object> params1 = New.arrayList();
            params1.add(i);
            params1.add("00000-" + i);
            params1.add("学生-" + i);
            params1.add(new Date());
            params1.add("18673922289");
            params1.add(1);
            params1.add(1);
            params1.add("北京市东花市北里20号楼6单元501室");
            params1.add(new Date());
            this.execute(sql1, params1);
            for (int j = 0; j < 7; j++) {
                List<Object> params2 = New.arrayList();
                params2.add(id2++);
                params2.add(j);
                params2.add("course-" + j);
                params2.add("000000" + j);
                int e = (int)(Math.random() * 100);
                params2.add(e);
                params2.add(null);
                params2.add(new Date());
                this.execute(sql2, params2);
            }
        }
    }
    
    
    
    
    
    
    
}
