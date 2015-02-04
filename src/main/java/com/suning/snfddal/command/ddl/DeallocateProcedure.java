/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * DEALLOCATE
 */
public class DeallocateProcedure extends DefineCommand {

    private String procedureName;

    public DeallocateProcedure(Session session) {
        super(session);
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DEALLOCATE;
    }

}
