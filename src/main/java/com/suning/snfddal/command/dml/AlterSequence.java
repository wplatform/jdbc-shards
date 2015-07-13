/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.dml;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.ddl.SchemaCommand;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.schema.Sequence;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;

/**
 * This class represents the statement
 * ALTER SEQUENCE
 */
public class AlterSequence extends SchemaCommand {

    private Table table;
    private Sequence sequence;
    private Expression start;
    private Expression increment;
    private Boolean cycle;
    private Expression minValue;
    private Expression maxValue;
    private Expression cacheSize;

    public AlterSequence(Session session, Schema schema) {
        super(session, schema);
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    public void setColumn(Column column) {
        table = column.getTable();
        sequence = column.getSequence();
        if (sequence == null) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, column.getSQL());
        }
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public void setCycle(Boolean cycle) {
        this.cycle = cycle;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_SEQUENCE;
    }

}
