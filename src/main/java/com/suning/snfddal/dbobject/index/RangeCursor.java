/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueLong;

/**
 * The cursor implementation for the range index.
 */
class RangeCursor implements Cursor {

    private boolean beforeFirst;
    private long current;
    private Row currentRow;
    private final long min, max;

    RangeCursor(long min, long max) {
        this.min = min;
        this.max = max;
        beforeFirst = true;
    }

    @Override
    public Row get() {
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            current = min;
        } else {
            current++;
        }
        currentRow = new Row(new Value[]{ValueLong.get(current)}, 1);
        return current <= max;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
