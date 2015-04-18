package com.suning.snfddal.shards.vendor;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.suning.snfddal.util.JdbcUtils;

/**
 * A MSSQLValidConnectionChecker.
 */
public class MSSQLValidConnectionChecker extends ValidConnectionCheckerAdapter implements
        ValidConnectionChecker, Serializable {

    private static final long serialVersionUID = 1L;

    public MSSQLValidConnectionChecker() {

    }

    public boolean isValidConnection(final Connection c, String validateQuery,
            int validationQueryTimeout) {
        try {
            if (c.isClosed()) {
                return false;
            }
        } catch (SQLException ex) {
            // skip
            return false;
        }

        Statement stmt = null;

        try {
            stmt = c.createStatement();
            stmt.setQueryTimeout(validationQueryTimeout);
            stmt.execute(validateQuery);
            return true;
        } catch (SQLException e) {
            return false;
        } finally {
            JdbcUtils.closeSilently(stmt);
        }
    }

}
