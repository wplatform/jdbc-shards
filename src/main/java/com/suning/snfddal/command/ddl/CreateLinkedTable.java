/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * CREATE LINKED TABLE
 */
public class CreateLinkedTable extends SchemaCommand {

    private String tableName;
    private String driver, url, user, password, originalSchema, originalTable;
    private boolean ifNotExists;
    private String comment;
    private boolean emitUpdates;
    private boolean force;
    private boolean temporary;
    private boolean globalTemporary;
    private boolean readOnly;

    public CreateLinkedTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setOriginalTable(String originalTable) {
        this.originalTable = originalTable;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setEmitUpdates(boolean emitUpdates) {
        this.emitUpdates = emitUpdates;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setTemporary(boolean temp) {
        this.temporary = temp;
    }

    public void setGlobalTemporary(boolean globalTemp) {
        this.globalTemporary = globalTemp;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setOriginalSchema(String originalSchema) {
        this.originalSchema = originalSchema;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_LINKED_TABLE;
    }

}
