package com.suning.snfddal.shards.vendor;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.suning.snfddal.util.JdbcUtils;
import com.suning.snfddal.util.Utils;

public class OracleValidConnectionChecker extends ValidConnectionCheckerAdapter implements
        ValidConnectionChecker, Serializable {

    private static final long serialVersionUID = -2227528634302168877L;
    private final Class<?> clazz;
    private final Method ping;
    private final static Object[] params = new Object[] { new Integer(5000) };

    public OracleValidConnectionChecker() {
        try {
            clazz = Utils.loadClass("oracle.jdbc.driver.OracleConnection");
            if (clazz != null) {
                ping = clazz.getMethod("pingDatabase", new Class[] { Integer.TYPE });
            } else {
                ping = null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to resolve pingDatabase method:", e);
        }

    }

    public void setTimeout(int timeout) {
        params[0] = timeout;
    }

    public boolean isValidConnection(Connection conn, String validateQuery,
            int validationQueryTimeout) {
        try {
            if (conn.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            // skip
            return false;
        }

        try {
            // unwrap
            if (clazz != null && clazz.isAssignableFrom(conn.getClass())) {
                Integer status = (Integer) ping.invoke(conn, params);

                // Error
                return status >= 0;
            }

            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery(validateQuery);
                return true;
            } catch (SQLException e) {
                return false;
            } catch (Exception e) {
                return false;
            } finally {
                JdbcUtils.closeSilently(rs);
                JdbcUtils.closeSilently(stmt);
            }
        } catch (Exception e) {
            return false;
        }
    }
}
