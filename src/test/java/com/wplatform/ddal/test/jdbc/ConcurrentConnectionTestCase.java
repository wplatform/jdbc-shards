package com.wplatform.ddal.test.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

import com.wplatform.ddal.test.BaseTestCase;
import com.wplatform.ddal.util.Task;

/**
 * Test concurrent usage of the same connection.
 */
public class ConcurrentConnectionTestCase extends BaseTestCase {


    @Test
    public void test() throws SQLException {
        testAutoCommit();
    }

    private void testAutoCommit() throws SQLException {
        final Connection conn = getConnection();
        final PreparedStatement p1 = conn.prepareStatement("select 1 from dual");
        Task t = new Task() {
            @Override
            public void call() throws Exception {
                while (!stop) {
                    p1.executeQuery();
                    conn.setAutoCommit(true);
                    conn.setAutoCommit(false);
                }
            }
        }.execute();
        PreparedStatement prep = conn.prepareStatement("select ? from dual");
        for (int i = 0; i < 10; i++) {
            prep.setBinaryStream(1, new ByteArrayInputStream(new byte[1024]));
            prep.executeQuery();
        }
        t.get();
        conn.close();
    }

}
