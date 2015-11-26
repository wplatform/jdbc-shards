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

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;

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

    public String getUserName() {
        return userName;
    }

    public boolean isAdmin() {
        return admin;
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

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getComment() {
        return comment;
    }
    

}
