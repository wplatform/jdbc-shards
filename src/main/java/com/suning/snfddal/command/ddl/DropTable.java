/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.engine.Constants;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends SchemaCommand {

    private boolean ifExists;
    private String tableName;
    private Table table;
    private DropTable next;
    private int dropAction;

    public DropTable(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                Constants.RESTRICT :
                    Constants.CASCADE;
    }

    /**
     * Chain another drop table statement to this statement.
     *
     * @param drop the statement to add
     */
    public void addNextDropTable(DropTable drop) {
        if (next == null) {
            next = drop;
        } else {
            next.addNextDropTable(drop);
        }
    }

    public void setIfExists(boolean b) {
        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
        if (next != null) {
            next.setDropAction(dropAction);
        }
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TABLE;
    }

}