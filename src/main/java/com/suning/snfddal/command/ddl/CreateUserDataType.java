/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * CREATE DOMAIN
 */
public class CreateUserDataType extends DefineCommand {

    private String typeName;
    private Column column;
    private boolean ifNotExists;

    public CreateUserDataType(Session session) {
        super(session);
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_DOMAIN;
    }

}
