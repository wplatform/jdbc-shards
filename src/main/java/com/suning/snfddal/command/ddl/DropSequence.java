/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.schema.Sequence;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;

/**
 * This class represents the statement
 * DROP SEQUENCE
 */
public class DropSequence extends SchemaCommand {

    private String sequenceName;
    private boolean ifExists;

    public DropSequence(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_SEQUENCE;
    }

}
