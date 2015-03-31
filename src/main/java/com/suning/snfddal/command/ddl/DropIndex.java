/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import java.util.ArrayList;

import com.suning.snfddal.api.ErrorCode;
import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.Right;
import com.suning.snfddal.dbobject.constraint.Constraint;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * DROP INDEX
 */
public class DropIndex extends SchemaCommand {

    private String indexName;
    private boolean ifExists;

    public DropIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_INDEX;
    }

}
