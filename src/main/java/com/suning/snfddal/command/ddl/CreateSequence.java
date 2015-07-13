/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * CREATE SEQUENCE
 */
public class CreateSequence extends SchemaCommand {

    private String sequenceName;
    private boolean ifNotExists;
    private boolean cycle;
    private Expression minValue;
    private Expression maxValue;
    private Expression start;
    private Expression increment;
    private Expression cacheSize;
    private boolean belongsToTable;

    public CreateSequence(Session session, Schema schema) {
        super(session, schema);
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SEQUENCE;
    }

}
