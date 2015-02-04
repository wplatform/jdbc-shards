/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.StringUtils;

/**
 * This class represents the statement
 * CREATE USER
 */
public class CreateUser extends DefineCommand {

    private String userName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean ifNotExists;
    private String comment;

    public CreateUser(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    private char[] getCharArray(Expression e) {
        return e.optimize(session).getValue(session).getString().toCharArray();
    }

    private byte[] getByteArray(Expression e) {
        return StringUtils.convertHexToBytes(
                e.optimize(session).getValue(session).getString());
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_USER;
    }

}
