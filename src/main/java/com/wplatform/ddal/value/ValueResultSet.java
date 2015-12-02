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
package com.wplatform.ddal.value;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.SimpleResultSet;
import com.wplatform.ddal.util.StatementBuilder;

/**
 * Implementation of the RESULT_SET data type.
 */
public class ValueResultSet extends Value {

    private final ResultSet result;

    private ValueResultSet(ResultSet rs) {
        this.result = rs;
    }

    /**
     * Create a result set value for the given result set.
     * The result set will be wrapped.
     *
     * @param rs the result set
     * @return the value
     */
    public static ValueResultSet get(ResultSet rs) {
        ValueResultSet val = new ValueResultSet(rs);
        return val;
    }

    /**
     * Create a result set value for the given result set. The result set will
     * be fully read in memory. The original result set is not closed.
     *
     * @param rs      the result set
     * @param maxrows the maximum number of rows to read (0 to just read the
     *                meta data)
     * @return the value
     */
    public static ValueResultSet getCopy(ResultSet rs, int maxrows) {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            SimpleResultSet simple = new SimpleResultSet();
            simple.setAutoClose(false);
            ValueResultSet val = new ValueResultSet(simple);
            for (int i = 0; i < columnCount; i++) {
                String name = meta.getColumnLabel(i + 1);
                int sqlType = meta.getColumnType(i + 1);
                int precision = meta.getPrecision(i + 1);
                int scale = meta.getScale(i + 1);
                simple.addColumn(name, sqlType, precision, scale);
            }
            for (int i = 0; i < maxrows && rs.next(); i++) {
                Object[] list = new Object[columnCount];
                for (int j = 0; j < columnCount; j++) {
                    list[j] = rs.getObject(j + 1);
                }
                simple.addRow(list);
            }
            return val;
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public int getType() {
        return Value.RESULT_SET;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        // it doesn't make sense to calculate it
        return Integer.MAX_VALUE;
    }

    @Override
    public String getString() {
        try {
            StatementBuilder buff = new StatementBuilder("(");
            result.beforeFirst();
            ResultSetMetaData meta = result.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 0; result.next(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append('(');
                buff.resetCount();
                for (int j = 0; j < columnCount; j++) {
                    buff.appendExceptFirst(", ");
                    int t = DataType.getValueTypeFromResultSet(meta, j + 1);
                    Value v = DataType.readValue(null, result, j + 1, t);
                    buff.append(v.getString());
                }
                buff.append(')');
            }
            result.beforeFirst();
            return buff.append(')').toString();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        return this == v ? 0 : super.toString().compareTo(v.toString());
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return result;
    }

    @Override
    public ResultSet getResultSet() {
        return result;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) {
        throw throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    @Override
    public String getSQL() {
        return "";
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        SimpleResultSet rs = new SimpleResultSet();
        rs.setAutoClose(false);
        return ValueResultSet.get(rs);
    }

}
