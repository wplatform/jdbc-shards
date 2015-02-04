/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import java.util.ArrayList;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.DbObject;
import com.suning.snfddal.dbobject.Role;
import com.suning.snfddal.dbobject.User;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.schema.SchemaObject;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.New;

/**
 * This class represents the statement
 * DROP ALL OBJECTS
 */
public class DropDatabase extends DefineCommand {

    private boolean dropAllObjects;
    private boolean deleteFiles;

    public DropDatabase(Session session) {
        super(session);
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setDropAllObjects(boolean b) {
        this.dropAllObjects = b;
    }

    public void setDeleteFiles(boolean b) {
        this.deleteFiles = b;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ALL_OBJECTS;
    }

}
