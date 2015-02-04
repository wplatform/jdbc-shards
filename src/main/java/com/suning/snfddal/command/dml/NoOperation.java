/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.dml;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.Prepared;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.result.ResultInterface;

/**
 * Represents an empty statement or a statement that has no effect.
 */
public class NoOperation extends Prepared {

    public NoOperation(Session session) {
        super(session);
    }

    @Override
    public int update() {
        return 0;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.NO_OPERATION;
    }

}
