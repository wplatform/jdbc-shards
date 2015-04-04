/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;

/**
 * The cursor implementation of a view index.
 */
public class ViewCursor implements Cursor {

    /* (non-Javadoc)
     * @see com.suning.snfddal.dbobject.index.Cursor#get()
     */
    @Override
    public Row get() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.dbobject.index.Cursor#getSearchRow()
     */
    @Override
    public SearchRow getSearchRow() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.dbobject.index.Cursor#next()
     */
    @Override
    public boolean next() {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.dbobject.index.Cursor#previous()
     */
    @Override
    public boolean previous() {
        // TODO Auto-generated method stub
        return false;
    }
    
}
