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
package com.wplatform.ddal.command.dml;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ExpressionColumn;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.LocalResult;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueString;

/**
 * This class represents the statement
 * EXPLAIN
 */
public class Explain extends Prepared {

    private Prepared command;
    private LocalResult result;
    private boolean executeCommand;

    public Explain(Session session) {
        super(session);
    }

    public void setCommand(Prepared command) {
        this.command = command;
    }

    @Override
    public void prepare() {
        command.prepare();
    }

    public void setExecuteCommand(boolean executeCommand) {
        this.executeCommand = executeCommand;
    }

    @Override
    public ResultInterface queryMeta() {
        return query(-1);
    }

    @Override
    public ResultInterface query(int maxrows) {
        Column column = new Column("PLAN", Value.STRING);
        Database db = session.getDatabase();
        ExpressionColumn expr = new ExpressionColumn(db, column);
        Expression[] expressions = {expr};
        result = new LocalResult(session, expressions, 1);
        if (maxrows >= 0) {
            String plan;
            if (executeCommand) {
                if (command.isQuery()) {
                    command.query(maxrows);
                } else {
                    command.update();
                }
                plan = command.getPlanSQL();
            } else {
                plan = command.getPlanSQL();
            }
            add(plan);
        }
        result.done();
        return result;
    }

    private void add(String text) {
        Value[] row = {ValueString.get(text)};
        result.addRow(row);
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return command.isReadOnly();
    }

    @Override
    public int getType() {
        return CommandInterface.EXPLAIN;
    }
}
