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
import com.suning.snfddal.util.StringUtils;

/**
 * This class represents the statement
 * CREATE ALIAS
 */
public class CreateFunctionAlias extends SchemaCommand {

    private String aliasName;
    private String javaClassMethod;
    private boolean deterministic;
    private boolean ifNotExists;
    private boolean force;
    private String source;
    private boolean bufferResultSetToLocalTemp = true;

    public CreateFunctionAlias(Session session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    /**
     * Set the qualified method name after removing whitespace.
     *
     * @param method the qualified method name
     */
    public void setJavaClassMethod(String method) {
        this.javaClassMethod = StringUtils.replaceAll(method, " ", "");
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     *
     * @param b the new value
     */
    public void setBufferResultSetToLocalTemp(boolean b) {
        this.bufferResultSetToLocalTemp = b;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_ALIAS;
    }

}
