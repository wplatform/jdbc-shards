/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.wplatform.ddal.test.sql.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;

/**
 * Test DROP statement
 */
public class TableDropTestCase extends BaseTestCase {

    private Connection conn;
    private Statement stat;


    @Test
    public void test() throws Exception {
        conn = getConnection();
        stat = conn.createStatement();

        testTableDependsOnView();
        testComputedColumnDependency();
        testInterSchemaDependency();

        conn.close();
    }

    private void testTableDependsOnView() throws SQLException {
        stat.execute("drop all objects");
        stat.execute("create table a(x int)");
        stat.execute("create view b as select * from a");
        stat.execute("create table c(y int check (select count(*) from b) = 0)");
        stat.execute("drop all objects");
    }

    private void testComputedColumnDependency() throws SQLException {
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE TABLE A (A INT);");
        stat.execute("CREATE TABLE B (B INT AS SELECT A FROM A);");
        stat.execute("DROP ALL OBJECTS");
        stat.execute("CREATE SCHEMA TEST_SCHEMA");
        stat.execute("CREATE TABLE TEST_SCHEMA.A (A INT);");
        stat.execute("CREATE TABLE TEST_SCHEMA.B " +
                "(B INT AS SELECT A FROM TEST_SCHEMA.A);");
        stat.execute("DROP SCHEMA TEST_SCHEMA");
    }

    private void testInterSchemaDependency() throws SQLException {
        stat.execute("drop all objects;");
        stat.execute("create schema table_view");
        stat.execute("set schema table_view");
        stat.execute("create table test1 (id int, name varchar(20))");
        stat.execute("create view test_view_1 as (select * from test1)");
        stat.execute("set schema public");
        stat.execute("create schema test_run");
        stat.execute("set schema test_run");
        stat.execute("create table test2 (id int, address varchar(20), " +
                "constraint a_cons check (id in (select id from table_view.test1)))");
        stat.execute("set schema public");
        stat.execute("drop all objects");
    }
}
