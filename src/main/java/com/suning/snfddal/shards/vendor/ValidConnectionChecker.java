package com.suning.snfddal.shards.vendor;

import java.sql.Connection;

public interface ValidConnectionChecker {

    boolean isValidConnection(Connection c, String query, int validationQueryTimeout);

}
