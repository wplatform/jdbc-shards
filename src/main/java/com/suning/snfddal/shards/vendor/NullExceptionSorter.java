package com.suning.snfddal.shards.vendor;

import java.sql.SQLException;

import com.suning.snfddal.shards.ExceptionSorter;

public class NullExceptionSorter implements ExceptionSorter {

    private final static NullExceptionSorter instance = new NullExceptionSorter();

    public final static NullExceptionSorter getInstance() {
        return instance;
    }

    @Override
    public boolean isExceptionFatal(SQLException e) {
        return false;
    }

}
