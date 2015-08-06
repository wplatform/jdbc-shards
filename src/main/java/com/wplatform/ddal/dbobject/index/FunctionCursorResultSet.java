/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wplatform.ddal.dbobject.index;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.value.DataType;
import com.wplatform.ddal.value.Value;

/**
 * A cursor for a function that returns a JDBC result set.
 */
public class FunctionCursorResultSet implements Cursor {

    private final Session session;
    private final ResultSet result;
    private final ResultSetMetaData meta;
    private Value[] values;
    private Row row;

    FunctionCursorResultSet(Session session, ResultSet result) {
        this.session = session;
        this.result = result;
        try {
            this.meta = result.getMetaData();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = new Row(values, 1);
        }
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        row = null;
        try {
            if (result != null && result.next()) {
                int columnCount = meta.getColumnCount();
                values = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    int type = DataType.getValueTypeFromResultSet(meta, i + 1);
                    values[i] = DataType.readValue(session, result, i + 1, type);
                }
            } else {
                values = null;
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        return values != null;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}