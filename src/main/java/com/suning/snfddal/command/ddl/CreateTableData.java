/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.util.New;

import java.util.ArrayList;

/**
 * The data required to create a table.
 */
public class CreateTableData {

    /**
     * The schema.
     */
    public Schema schema;

    /**
     * The table name.
     */
    public String tableName;

    /**
     * The object id.
     */
    public int id;

    /**
     * The column list.
     */
    public ArrayList<Column> columns = New.arrayList();

    /**
     * Whether this is a temporary table.
     */
    public boolean temporary;

    /**
     * Whether the table is global temporary.
     */
    public boolean globalTemporary;

    /**
     * Whether to create a new table.
     */
    public boolean create;

    /**
     * The session.
     */
    public Session session;

    /**
     * The table engine to use for creating the table.
     */
    public String tableEngine;

    /**
     * The table engine params to use for creating the table.
     */
    public ArrayList<String> tableEngineParams;

    /**
     * The table is hidden.
     */
    public boolean isHidden;

}
