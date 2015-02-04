/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * CREATE CONSTANT
 */
public class CreateConstant extends SchemaCommand {

    private String constantName;
    private Expression expression;
    private boolean ifNotExists;

    public CreateConstant(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    public void setExpression(Expression expr) {
        this.expression = expr;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_CONSTANT;
    }

}
