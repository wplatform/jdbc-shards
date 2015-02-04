/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.api;

import com.suning.snfddal.command.ddl.CreateTableData;
import com.suning.snfddal.dbobject.table.Table;

/**
 * A class that implements this interface can create custom table
 * implementations.
 *
 * @author Sergi Vladykin
 */
public interface TableEngine {

    /**
     * Create new table.
     *
     * @param data the data to construct the table
     * @return the created table
     */
    Table createTable(CreateTableData data);

}
