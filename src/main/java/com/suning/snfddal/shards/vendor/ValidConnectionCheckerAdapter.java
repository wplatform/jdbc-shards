package com.suning.snfddal.shards.vendor;

import java.sql.Connection;

/**
 * @author wenshao<szujobs@hotmail.com>
 * @since 0.2.21
 */
public class ValidConnectionCheckerAdapter implements ValidConnectionChecker {

    @Override
    public boolean isValidConnection(Connection c, String query, int validationQueryTimeout) {
        return true;
    }

}
