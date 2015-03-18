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
// Created on 2014年11月17日
// $Id$

package com.suning.snfddal.test.query;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.suning.snfddal.test.BaseSampleCase;
import com.suning.snfddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class QueryTestCase extends BaseSampleCase{
    
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
    public void testQuery_SimpleAggregate(){
        String sql = "SELECT count(*), max(f_student_id), avg(t_score),sum(t_score) FROM t_student_course";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_Count(){
        String sql = "SELECT count(1) FROM t_student_course";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_Count1(){
        String sql = "SELECT count(DISTINCT f_student_id) FROM t_student_course";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_distinct(){
        String sql = "SELECT DISTINCT f_id, f_student_id, t_course_name, f_course_no, t_score, t_learn_year, f_gmt FROM t_student_course";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_Aggregate_GroupBy(){
        String sql = " SELECT  f_student_id, max(t_score), avg(t_score), count(1) FROM t_student_course group by f_student_id";
        this.query_Sql(sql, null);
    }


    @Test
    public void testQuery_SimpleOR(){
        String sql = "SELECT * FROM t_student where f_student_id > 3 or f_school_id < 100";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SimpleAnd(){
        String sql = "SELECT * FROM t_student where f_student_id > 3 and F_SCHOOL_ID < 100";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_UnEvaluatableConddition(){
        String sql = "SELECT * FROM t_student where f_student_id = F_SCHOOL_ID ";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_AlwaysFlaseConddition(){
        String sql = "SELECT * FROM t_student where f_student_id = 8 and f_student_id < 5 ";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_HalfFlaseConddition(){
        String sql = "SELECT * FROM t_student where f_student_id IN (8,9,10) and f_student_id > 8 ";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SimpleInnerJoinOr(){
        String sql = "SELECT * FROM t_student a inner join t_student_course b on a.f_student_id = b.f_student_id  where a.f_student_id > 3 or b.f_id < 100";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SimpleInnerJoinAnd(){
        String sql = "SELECT * FROM t_student a inner join t_student_course b on a.f_student_id = b.f_student_id  where a.f_student_id > ? and b.f_id < ?";
        List<Object> args = New.arrayList();
        args.add(3);
        args.add(100);
        this.query_Sql(sql, args);
    }
    
    @Test
    public void testQuery_SubQueryIn(){
        String sql = "SELECT * FROM t_student where f_student_id in (select f_student_id from t_student_course where t_score > 80)";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SubQueryEqAny(){
        String sql = "SELECT * FROM t_student where f_student_id = any(select f_student_id from t_student_course where t_score > 80)";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SubQueryEq(){
        String sql = "SELECT * FROM t_student where f_student_id = (select f_student_id from t_student_course where t_score = 80)";
        this.query_Sql(sql, null);
    }
    
    @Test
    public void testQuery_SubQueryExists(){
        String sql = "SELECT * FROM t_student a where exists(select 1 from t_student_course where t_score > 80 and a.f_student_id = f_student_id)";
        this.query_Sql(sql, null);
    }
    
    
    @Test
    public void testQuery_SubQueryExists2(){
        String sql = "SELECT * FROM t_student a where exists(select 1 from t_student_course where t_score > 80)";
        this.query_Sql(sql, null);
    }
    
    
    @Test
    public void testQuery_SubQuery(){
        String sql = "SELECT * FROM (select * from t_student_course where t_score > 80) a, t_student b where a.f_student_id = b.f_student_id";
        this.query_Sql(sql, null);
    }
    
    
  
}
