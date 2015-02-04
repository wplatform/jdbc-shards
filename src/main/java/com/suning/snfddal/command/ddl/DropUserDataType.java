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
 * DROP DOMAIN
 */
public class DropUserDataType extends DefineCommand {

    private String typeName;
    private boolean ifExists;

    public DropUserDataType(Session session) {
        super(session);
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
