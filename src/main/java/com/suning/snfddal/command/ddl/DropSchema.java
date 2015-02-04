/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.api.ErrorCode;
import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * DROP SCHEMA
 */
public class DropSchema extends DefineCommand {

    private String schemaName;
    private boolean ifExists;

    public DropSchema(Session session) {
        super(session);
    }

    public void setSchemaName(String name) {
        this.schemaName = name;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_SCHEMA;
    }

}
