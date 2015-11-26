/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplatform.ddal.command.ddl;

import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.User;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

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

    public void setType(int type) {
        this.type = type;
    }

    public User getUser() {
        return user;
    }

    public String getNewName() {
        return newName;
    }

    public Expression getPassword() {
        return password;
    }

    public Expression getSalt() {
        return salt;
    }

    public Expression getHash() {
        return hash;
    }

    public boolean isAdmin() {
        return admin;
    }
    
    

}
