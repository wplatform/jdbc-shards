/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command;

import java.util.ArrayList;

import com.suning.snfddal.command.expression.Parameter;
import com.suning.snfddal.command.expression.ParameterInterface;
import com.suning.snfddal.result.ResultInterface;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueNull;

/**
 * Represents a single SQL statements.
 * It wraps a prepared statement.
 */
class CommandContainer extends Command {

    private Prepared prepared;
    private boolean readOnlyKnown;
    private boolean readOnly;

    CommandContainer(Parser parser, String sql, Prepared prepared) {
        super(parser, sql);
        prepared.setCommand(this);
        this.prepared = prepared;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return prepared.getParameters();
    }

    @Override
    public boolean isTransactional() {
        return prepared.isTransactional();
    }

    @Override
    public boolean isQuery() {
        return prepared.isQuery();
    }

    private void recompileIfRequired() {
        if (prepared.needRecompile()) {
            // TODO test with 'always recompile'
            String sql = prepared.getSQL();
            ArrayList<Parameter> oldParams = prepared.getParameters();
            Parser parser = new Parser(session);
            prepared = parser.parse(sql);
            ArrayList<Parameter> newParams = prepared.getParameters();
            for (int i = 0, size = newParams.size(); i < size; i++) {
                Parameter old = oldParams.get(i);
                if (old.isValueSet()) {
                    Value v = old.getValue(session);
                    Parameter p = newParams.get(i);
                    p.setValue(v);
                }
            }
            prepared.prepare();
        }
    }

    @Override
    public int update() {
        recompileIfRequired();
        start();
        session.setLastScopeIdentity(ValueNull.INSTANCE);
        prepared.checkParameters();
        int updateCount = prepared.update();
        prepared.trace(startTime, updateCount);
        return updateCount;
    }

    @Override
    public ResultInterface query(int maxrows) {
        recompileIfRequired();
        start();
        prepared.checkParameters();
        ResultInterface result = prepared.query(maxrows);
        prepared.trace(startTime, result.getRowCount());
        return result;
    }

    @Override
    public boolean isReadOnly() {
        if (!readOnlyKnown) {
            readOnly = prepared.isReadOnly();
            readOnlyKnown = true;
        }
        return readOnly;
    }

    @Override
    public ResultInterface queryMeta() {
        return prepared.queryMeta();
    }

    @Override
    public boolean isCacheable() {
        return prepared.isCacheable();
    }

    @Override
    public int getCommandType() {
        return prepared.getType();
    }

}
