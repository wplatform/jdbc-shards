package com.suning.snfddal.shards.vendor;

import com.suning.snfddal.shards.ExceptionSorter;

import java.sql.SQLException;

public class DB2ExceptionSorter implements ExceptionSorter {

    @Override
    public boolean isExceptionFatal(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState != null && sqlState.startsWith("08")) { // Connection Exception
            return true;
        }

        int errorCode = e.getErrorCode();
        switch (errorCode) {
            case -512: // STATEMENT REFERENCE TO REMOTE OBJECT IS INVALID
            case -514: // THE CURSOR IS NOT IN A PREPARED STATE
            case -516: // THE DESCRIBE STATEMENT DOES NOT SPECIFY A PREPARED STATEMENT
            case -518: // THE EXECUTE STATEMENT DOES NOT IDENTIFY A VALID PREPARED STATEMENT
            case -525: // THE SQL STATEMENT CANNOT BE EXECUTED BECAUSE IT WAS IN ERROR AT BIND TIME FOR SECTION = sectno
                // PACKAGE = pkgname CONSISTENCY TOKEN = contoken
            case -909: // THE OBJECT HAS BEEN DELETED OR ALTERED
            case -918: // THE SQL STATEMENT CANNOT BE EXECUTED BECAUSE A CONNECTION HAS BEEN LOST
            case -924: // DB2 CONNECTION INTERNAL ERROR, function-code,return-code,reason-code
                return true;
            default:
                break;
        }
        return false;
    }


}
