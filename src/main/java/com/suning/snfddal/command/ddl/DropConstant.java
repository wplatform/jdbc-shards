/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Constant;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;

/**
 * This class represents the statement
 * DROP CONSTANT
 */
public class DropConstant extends SchemaCommand {

    private String constantName;
    private boolean ifExists;

    public DropConstant(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_CONSTANT;
    }

}
