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
import java.sql.SQLException;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.MathUtils;

/**
 * Implementation of the SMALLINT data type.
 */
public class ValueShort extends Value {

    /**
     * The precision in digits.
     */
    static final int PRECISION = 5;

    /**
     * The maximum display size of a short.
     * Example: -32768
     */
    static final int DISPLAY_SIZE = 6;

    private final short value;

    private ValueShort(short value) {
        this.value = value;
    }

    private static ValueShort checkRange(int x) {
        if (x < Short.MIN_VALUE || x > Short.MAX_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                    Integer.toString(x));
        }
        return ValueShort.get((short) x);
    }

    /**
     * Get or create a short value for the given short.
     *
     * @param i the short
     * @return the value
     */
    public static ValueShort get(short i) {
        return (ValueShort) Value.cache(new ValueShort(i));
    }

    @Override
    public Value add(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value + other.value);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(int) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value - other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueShort other = (ValueShort) v;
        return checkRange(value * other.value);
    }

    @Override
    public Value divide(Value v) {
        ValueShort other = (ValueShort) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueShort.get((short) (value / other.value));
    }

    @Override
    public Value modulus(Value v) {
        ValueShort other = (ValueShort) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueShort.get((short) (value % other.value));
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getType() {
        return Value.SHORT;
    }

    @Override
    public short getShort() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueShort v = (ValueShort) o;
        return MathUtils.compareInt(value, v.value);
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public Object getObject() {
        return Short.valueOf(value);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setShort(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueShort && value == ((ValueShort) other).value;
    }

}
