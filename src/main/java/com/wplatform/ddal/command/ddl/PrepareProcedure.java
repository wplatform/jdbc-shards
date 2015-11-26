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

import java.util.ArrayList;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.command.expression.Parameter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.New;

/**
 * This class represents the statement
 * PREPARE
 */
public class PrepareProcedure extends DefineCommand {

    private String procedureName;
    private Prepared prepared;

    public PrepareProcedure(Session session) {
        super(session);
    }

    @Override
    public void checkParameters() {
        // no not check parameters
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    public void setPrepared(Prepared prep) {
        this.prepared = prep;
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return New.arrayList();
    }

    @Override
    public int getType() {
        return CommandInterface.PREPARE;
    }

    public String getProcedureName() {
        return procedureName;
    }

    public Prepared getPrepared() {
        return prepared;
    }

}
