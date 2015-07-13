/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.dbobject.table.TableMate;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;

/**
 * This class represents a non-transaction statement that involves a schema.
 */
public abstract class SchemaCommand extends DefineCommand {

    private final Schema schema;

    /**
     * Create a new command.
     *
     * @param session the session
     * @param schema  the schema
     */
    public SchemaCommand(Session session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    /**
     * Get the schema
     *
     * @return the schema
     */
    protected Schema getSchema() {
        return schema;
    }


    /**
     * @param tableName
     */
    public TableMate finalTableMate(String tableName) {
        Table tableOrView = getSchema().findTableOrView(session, tableName);
        TableMate tableMate = null;
        if (tableOrView != null && tableOrView instanceof TableMate) {
            tableMate = (TableMate) tableOrView;
        }
        return tableMate;
    }


    /**
     * @param tableOrView
     */
    public TableMate getTableMate(String tableName) {
        Table tableOrView = getSchema().getTableOrView(session, tableName);
        if (tableOrView != null && tableOrView instanceof TableMate) {
            return (TableMate) tableOrView;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

}
