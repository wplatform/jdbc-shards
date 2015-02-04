/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.constraint.ConstraintReferential;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * DROP VIEW
 */
public class DropView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;
    private int dropAction;

    public DropView(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintReferential.RESTRICT :
                ConstraintReferential.CASCADE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_VIEW;
    }

}
