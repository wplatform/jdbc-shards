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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;

import com.wplatform.ddal.message.DbException;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * This special instance is used as a marker for deleted entries in a map.
     * It should not be used anywhere else.
     */
    public static final ValueNull DELETED = new ValueNull();

    /**
     * The precision of NULL.
     */
    private static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    private static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    @Override
    public String getSQL() {
        return "NULL";
    }

    @Override
    public int getType() {
        return Value.NULL;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public Boolean getBoolean() {
        return null;
    }

    @Override
    public Date getDate() {
        return null;
    }

    @Override
    public Time getTime() {
        return null;
    }

    @Override
    public Timestamp getTimestamp() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public short getShort() {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return null;
    }

    @Override
    public double getDouble() {
        return 0.0;
    }

    @Override
    public float getFloat() {
        return 0.0F;
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public Reader getReader() {
        return null;
    }

    @Override
    public Value convertTo(int type) {
        return this;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        throw DbException.throwInternalError("compare null");
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setNull(parameterIndex, DataType.convertTypeToSQLType(Value.NULL));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

}
