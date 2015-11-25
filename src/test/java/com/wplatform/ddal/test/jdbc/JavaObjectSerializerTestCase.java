/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.wplatform.ddal.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;
import com.wplatform.ddal.util.JavaObjectSerializer;
import com.wplatform.ddal.util.JdbcUtils;

/**
 * Tests {@link JavaObjectSerializer}.
 *
 * @author Sergi Vladykin
 * @author Davide Cavestro
 */
public class JavaObjectSerializerTestCase extends BaseTestCase {



    @Test
    public void test() throws Exception {
        testStaticGlobalSerializer();
        testDbLevelJavaObjectSerializer();
    }

    private void testStaticGlobalSerializer() throws Exception {
        JdbcUtils.serializer = new JavaObjectSerializer() {
            @Override
            public byte[] serialize(Object obj) throws Exception {
                assertEquals(100500, ((Integer) obj).intValue());

                return new byte[] { 1, 2, 3 };
            }

            @Override
            public Object deserialize(byte[] bytes) throws Exception {
                assertEquals(new byte[] { 1, 2, 3 }, bytes);

                return 100500;
            }
        };

        try {
            Connection conn = getConnection();

            Statement stat = conn.createStatement();
            stat.execute("create table t(id identity, val other)");

            PreparedStatement ins = conn.prepareStatement("insert into t(val) values(?)");

            ins.setObject(1, 100500, Types.JAVA_OBJECT);
            assertEquals(1, ins.executeUpdate());

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select val from t");

            assertTrue(rs.next());

            assertEquals(100500, ((Integer) rs.getObject(1)).intValue());
            assertEquals(new byte[] { 1, 2, 3 }, rs.getBytes(1));

            conn.close();
        } finally {
            JdbcUtils.serializer = null;
        }
    }

    /**
     * Tests per-database serializer when set through the related SET command.
     */
    public void testDbLevelJavaObjectSerializer() throws Exception {

        DbLevelJavaObjectSerializer.testBaseRef = this;

        try {
            Connection conn = getConnection();

            conn.createStatement().execute("SET JAVA_OBJECT_SERIALIZER '"+
                    DbLevelJavaObjectSerializer.class.getName()+"'");

            Statement stat = conn.createStatement();
            stat.execute("create table t1(id identity, val other)");

            PreparedStatement ins = conn.prepareStatement("insert into t1(val) values(?)");

            ins.setObject(1, 100500, Types.JAVA_OBJECT);
            assertEquals(1, ins.executeUpdate());

            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select val from t1");

            assertTrue(rs.next());

            assertEquals(100500, ((Integer) rs.getObject(1)).intValue());
            assertEquals(new byte[] { 1, 2, 3 }, rs.getBytes(1));

            conn.close();
        } finally {
            DbLevelJavaObjectSerializer.testBaseRef = null;
        }
    }

    /**
     * The serializer to use for this test.
     */
    public static class DbLevelJavaObjectSerializer implements
            JavaObjectSerializer {

        /**
         * The test.
         */
        static BaseTestCase testBaseRef;

        @Override
        public byte[] serialize(Object obj) throws Exception {
            testBaseRef.assertEquals(100500, ((Integer) obj).intValue());

            return new byte[] { 1, 2, 3 };
        }

        @Override
        public Object deserialize(byte[] bytes) throws Exception {
            testBaseRef.assertEquals(new byte[] { 1, 2, 3 }, bytes);

            return 100500;
        }

    }
}
