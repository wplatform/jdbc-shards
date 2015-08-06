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
package com.wplatform.ddal.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.TraceObject;
import com.wplatform.ddal.result.SimpleResultSet;
import com.wplatform.ddal.value.Value;

/**
 * Represents an ARRAY value.
 */
public class JdbcArray extends TraceObject implements Array {

    private final JdbcConnection conn;
    private Value value;

    /**
     * INTERNAL
     */
    JdbcArray(JdbcConnection conn, Value value, int id) {
        setTrace(conn.getSession().getTrace(), TraceObject.ARRAY, id);
        this.conn = conn;
        this.value = value;
    }

    private static ResultSet getResultSet(Object[] array, long offset) {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("INDEX", Types.BIGINT, 0, 0);
        // TODO array result set: there are multiple data types possible
        rs.addColumn("VALUE", Types.NULL, 0, 0);
        for (int i = 0; i < array.length; i++) {
            rs.addRow(Long.valueOf(offset + i + 1), array[i]);
        }
        return rs;
    }

    /**
     * Returns the value as a Java array.
     * This method always returns an Object[].
     *
     * @return the Object array
     */
    @Override
    public Object getArray() throws SQLException {
        try {
            debugCodeCall("getArray");
            checkClosed();
            return get();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array.
     * This method always returns an Object[].
     *
     * @param map is ignored. Only empty or null maps are supported
     * @return the Object array
     */
    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        try {
            debugCode("getArray(" + quoteMap(map) + ");");
            JdbcConnection.checkMap(map);
            checkClosed();
            return get();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array. A subset of the array is returned,
     * starting from the index (1 meaning the first element) and up to the given
     * object count. This method always returns an Object[].
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @return the Object array
     */
    @Override
    public Object getArray(long index, int count) throws SQLException {
        try {
            debugCode("getArray(" + index + ", " + count + ");");
            checkClosed();
            return get(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a Java array. A subset of the array is returned,
     * starting from the index (1 meaning the first element) and up to the given
     * object count. This method always returns an Object[].
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @param map   is ignored. Only empty or null maps are supported
     * @return the Object array
     */
    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map)
            throws SQLException {
        try {
            debugCode("getArray(" + index + ", " + count + ", " + quoteMap(map) + ");");
            checkClosed();
            JdbcConnection.checkMap(map);
            return get(index, count);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type of the array. This database does support mixed type
     * arrays and therefore there is no base type.
     *
     * @return Types.NULL
     */
    @Override
    public int getBaseType() throws SQLException {
        try {
            debugCodeCall("getBaseType");
            checkClosed();
            return Types.NULL;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the base type name of the array. This database does support mixed
     * type arrays and therefore there is no base type.
     *
     * @return "NULL"
     */
    @Override
    public String getBaseTypeName() throws SQLException {
        try {
            debugCodeCall("getBaseTypeName");
            checkClosed();
            return "NULL";
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set.
     * The first column contains the index
     * (starting with 1) and the second column the value.
     *
     * @return the result set
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            debugCodeCall("getResultSet");
            checkClosed();
            return getResultSet(get(), 0);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set. The first column contains the index
     * (starting with 1) and the second column the value.
     *
     * @param map is ignored. Only empty or null maps are supported
     * @return the result set
     */
    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        try {
            debugCode("getResultSet(" + quoteMap(map) + ");");
            checkClosed();
            JdbcConnection.checkMap(map);
            return getResultSet(get(), 0);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set. The first column contains the index
     * (starting with 1) and the second column the value. A subset of the array
     * is returned, starting from the index (1 meaning the first element) and
     * up to the given object count.
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @return the result set
     */
    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        try {
            debugCode("getResultSet(" + index + ", " + count + ");");
            checkClosed();
            return getResultSet(get(index, count), index - 1);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value as a result set.
     * The first column contains the index
     * (starting with 1) and the second column the value.
     * A subset of the array is returned, starting from the index
     * (1 meaning the first element) and up to the given object count.
     *
     * @param index the start index of the subset (starting with 1)
     * @param count the maximum number of values
     * @param map   is ignored. Only empty or null maps are supported
     * @return the result set
     */
    @Override
    public ResultSet getResultSet(long index, int count,
                                  Map<String, Class<?>> map) throws SQLException {
        try {
            debugCode("getResultSet(" + index + ", " + count + ", " + quoteMap(map) + ");");
            checkClosed();
            JdbcConnection.checkMap(map);
            return getResultSet(get(index, count), index - 1);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Release all resources of this object.
     */
    @Override
    public void free() {
        debugCodeCall("free");
        value = null;
    }

    private void checkClosed() {
        conn.checkClosed();
        if (value == null) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
    }

    private Object[] get() {
        return (Object[]) value.convertTo(Value.ARRAY).getObject();
    }

    private Object[] get(long index, int count) {
        Object[] array = get();
        if (count < 0 || count > array.length) {
            throw DbException.getInvalidValueException("count (1.."
                    + array.length + ")", count);
        }
        if (index < 1 || index > array.length) {
            throw DbException.getInvalidValueException("index (1.."
                    + array.length + ")", index);
        }
        Object[] subset = new Object[count];
        System.arraycopy(array, (int) (index - 1), subset, 0, count);
        return subset;
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return value == null ? "null" :
                (getTraceObjectName() + ": " + value.getTraceSQL());
    }
}
