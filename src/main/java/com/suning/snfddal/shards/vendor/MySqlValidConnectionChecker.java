package com.suning.snfddal.shards.vendor;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.suning.snfddal.util.JdbcUtils;
import com.suning.snfddal.util.Utils;

public class MySqlValidConnectionChecker extends ValidConnectionCheckerAdapter implements
        ValidConnectionChecker, Serializable {

    private static final long serialVersionUID = 1L;

    private Class<?> clazz;
    private Method ping;
    private boolean usePingMethod = false;

    public MySqlValidConnectionChecker() {
        try {
            clazz = Utils.loadClass("com.mysql.jdbc.Connection");
            ping = clazz.getMethod("ping");
            if (ping != null) {
                usePingMethod = true;
            }
        } catch (Exception e) {
            // LOG.warn("Cannot resolve com.mysq.jdbc.Connection.ping method.  Will use 'SELECT 1' instead.",
            // e);
        }

    }

    public boolean isUsePingMethod() {
        return usePingMethod;
    }

    public void setUsePingMethod(boolean usePingMethod) {
        this.usePingMethod = usePingMethod;
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

        if (usePingMethod) {
            if (clazz.isAssignableFrom(conn.getClass())) {
                try {
                    ping.invoke(conn);
                    return true;
                } catch (InvocationTargetException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof SQLException) {
                        return false;
                    }

                    // LOG.warn("Unexpected error in ping", e);
                    return false;
                } catch (Exception e) {
                    // LOG.warn("Unexpected error in ping", e);
                    return false;
                }
            }
        }

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            if (validationQueryTimeout > 0) {
                stmt.setQueryTimeout(validationQueryTimeout);
            }
            rs = stmt.executeQuery(validateQuery);
            return true;
        } catch (SQLException e) {
            return false;
        } catch (Exception e) {
            // LOG.warn("Unexpected error in ping", e);
            return false;
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
        }

    }

}
