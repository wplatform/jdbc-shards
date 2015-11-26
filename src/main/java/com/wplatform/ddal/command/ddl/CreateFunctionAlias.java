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
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.StringUtils;

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

    public String getAliasName() {
        return aliasName;
    }

    public String getJavaClassMethod() {
        return javaClassMethod;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isForce() {
        return force;
    }

    public String getSource() {
        return source;
    }

    public boolean isBufferResultSetToLocalTemp() {
        return bufferResultSetToLocalTemp;
    }
    
    

}
