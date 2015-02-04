/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.result;

import java.util.ArrayList;

import com.suning.snfddal.engine.Constants;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.New;

/**
 * A list of rows. If the list grows too large, it is buffered to disk
 * automatically.
 */
public class RowList {

    private final Session session;
    private final ArrayList<Row> list = New.arrayList();
    private int size;
    private int index;
    private final int maxMemory;
    private int memory;

    /**
     * Construct a new row list for this session.
     *
     * @param session the session
     */
    public RowList(Session session) {
        this.session = session;
        maxMemory = session.getDatabase().getMaxOperationMemory();
    }


    private void writeAllRows() {
        session.getId();
        throw DbException.getUnsupportedException("TODO");
    }

    /**
     * Add a row to the list.
     *
     * @param r the row to add
     */
    public void add(Row r) {
        list.add(r);
        memory += r.getMemory() + Constants.MEMORY_POINTER;
        if (maxMemory > 0 && memory > maxMemory) {
            writeAllRows();
        }
        size++;
    }

    /**
     * Remove all rows from the list.
     */
    public void reset() {
        index = 0;
    }

    /**
     * Check if there are more rows in this list.
     *
     * @return true it there are more rows
     */
    public boolean hasNext() {
        return index < size;
    }

    /**
     * Get the next row from the list.
     *
     * @return the next row
     */
    public Row next() {
        Row r = list.get(index++);
        return r;
    }

    /**
     * Get the number of rows in this list.
     *
     * @return the number of rows
     */
    public int size() {
        return size;
    }

    /**
     * Close the result list and delete the temporary file.
     */
    public void close() {

    }

}
