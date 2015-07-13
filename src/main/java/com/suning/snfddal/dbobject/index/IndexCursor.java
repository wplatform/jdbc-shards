/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;

import java.util.ArrayList;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class IndexCursor implements Cursor {

    private final TableFilter tableFilter;
    private Session session;
    private Index index;
    private Table table;
    private IndexColumn[] indexColumns;
    private boolean alwaysFalse;
    private SearchRow start, end;
    private Cursor cursor;

    public IndexCursor(TableFilter filter) {
        this.tableFilter = filter;
    }

    public void setIndex(Index index) {
        this.index = index;
        this.table = index.getTable();
        Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    /**
     * Re-evaluate the start and end values of the index search for rows.
     *
     * @param s               the session
     * @param indexConditions the index conditions
     */
    public void find(Session s, ArrayList<IndexCondition> indexConditions) {
        Expression filter = tableFilter.getFilterCondition();
        if (filter == null) {

        }
    }

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        if (cursor == null) {
            return false;
        }
        return cursor.next();
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

}
