/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.User;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statements
 * ALTER USER ADMIN,
 * ALTER USER RENAME,
 * ALTER USER SET PASSWORD
 */
public class AlterUser extends DefineCommand {

    private int type;
    private User user;
    private String newName;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean admin;

    public AlterUser(Session session) {
        super(session);
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return type;
    }

}
