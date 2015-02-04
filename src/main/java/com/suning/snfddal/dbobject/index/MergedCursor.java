/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import java.util.List;

import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class MergedCursor implements Cursor {
    
    private List<ResultCursor> cursors;
    private ResultCursor currentCursor;
    private Row currentRow;
    private int index = 0;

    public MergedCursor(List<ResultCursor> cursors) {
        super();
        this.cursors = cursors;
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
        while (index < cursors.size()) {
            if(currentCursor == null) {
                currentCursor = cursors.get(index);
            }
            boolean result = currentCursor.next();
            if (!result) {
                currentCursor = null;
                ++ index;
            } else {
                currentRow = currentCursor.get();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }
    
}
