/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.MappedTable;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.result.SearchRow;
import com.suning.snfddal.value.DataType;
import com.suning.snfddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ResultCursor implements Cursor {

    private final MappedTable table;
    private final Session session;
    private final ResultSet rs;
    private Row current;

    ResultCursor(MappedTable table, ResultSet rs, Session session) {
        this.session = session;
        this.table = table;
        this.rs = rs;
    }

    @Override
    public Row get() {
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return current;
    }

    @Override
    public boolean next() {
        try {
            boolean result = rs.next();
            if (!result) {
                rs.close();
                //table.reusePreparedStatement(prep, sql);
                current = null;
                return false;
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        current = table.getTemplateRow();
        for (int i = 0; i < current.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            Value v = DataType.readValue(session, rs, i + 1, col.getType());
            current.setValue(i, v);
        }
        return true;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
