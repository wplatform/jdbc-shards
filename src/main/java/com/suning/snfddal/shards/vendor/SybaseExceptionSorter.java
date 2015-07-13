package com.suning.snfddal.shards.vendor;

import com.suning.snfddal.shards.ExceptionSorter;

import java.io.Serializable;
import java.sql.SQLException;

public class SybaseExceptionSorter implements ExceptionSorter, Serializable {

    private static final long serialVersionUID = 2742592563671255116L;

    public boolean isExceptionFatal(SQLException e) {
        boolean result = false;

        String errorText = e.getMessage();
        if (errorText == null) {
            return false;
        }
        errorText = errorText.toUpperCase();

        if ((errorText.contains("JZ0C0")) || // ERR_CONNECTION_DEAD
                (errorText.contains("JZ0C1")) // ERR_IOE_KILLED_CONNECTION
                ) {
            result = true;
        }

        return result;
    }

}
