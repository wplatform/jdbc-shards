/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import com.suning.snfddal.dbobject.table.FunctionTable;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.result.SortOrder;

/**
 * An index for a function that returns a result set. This index can only scan
 * through all rows, search is not supported.
 */
public class FunctionIndex extends BaseIndex {

    private final FunctionTable functionTable;

    public FunctionIndex(FunctionTable functionTable, IndexColumn[] columns) {
        initBaseIndex(functionTable, 0, null, columns, IndexType.createNonUnique(true));
        this.functionTable = functionTable;
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    @Override
    public void add(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void remove(Session session, Row row) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        if (functionTable.isBufferResultSetToLocalTemp()) {
            return new FunctionCursor(functionTable.getResult(session));
        }
        return new FunctionCursorResultSet(session,
                functionTable.getResultSet(session));
    }

    @Override
    public double getCost(Session session, int[] masks, TableFilter filter,
            SortOrder sortOrder) {
        if (masks != null) {
            throw DbException.getUnsupportedException("ALIAS");
        }
        long expectedRows;
        if (functionTable.canGetRowCount()) {
            expectedRows = functionTable.getRowCountApproximation();
        } else {
            expectedRows = database.getSettings().estimatedFunctionTableRows;
        }
        return expectedRows * 10;
    }

    @Override
    public void remove(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public void truncate(Session session) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("ALIAS");
    }

    @Override
    public long getRowCount(Session session) {
        return functionTable.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return functionTable.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public String getPlanSQL() {
        return "function";
    }

    @Override
    public boolean canScan() {
        return false;
    }

}
