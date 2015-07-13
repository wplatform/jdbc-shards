/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.dml;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.Prepared;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.command.expression.ExpressionColumn;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.result.LocalResult;
import com.suning.snfddal.result.ResultInterface;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueString;

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
