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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.MathUtils;

/**
 * Implementation of the BIGINT data type.
 */
public class ValueLong extends Value {

    /**
     * The largest Long value, as a BigInteger.
     */
    public static final BigInteger MAX = BigInteger.valueOf(Long.MAX_VALUE);

    /**
     * The smallest Long value, as a BigDecimal.
     */
    public static final BigDecimal MIN_BD = BigDecimal.valueOf(Long.MIN_VALUE);

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 19;

    /**
     * The maximum display size of a long.
     * Example: 9223372036854775808
     */
    public static final int DISPLAY_SIZE = 20;

    private static final BigInteger MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final int STATIC_SIZE = 100;
    private static final ValueLong[] STATIC_CACHE;

    static {
        STATIC_CACHE = new ValueLong[STATIC_SIZE];
        for (int i = 0; i < STATIC_SIZE; i++) {
            STATIC_CACHE[i] = new ValueLong(i);
        }
    }

    private final long value;

    private ValueLong(long value) {
        this.value = value;
    }

    private static boolean isInteger(long a) {
        return a >= Integer.MIN_VALUE && a <= Integer.MAX_VALUE;
    }

    /**
     * Get or create a long value for the given long.
     *
     * @param i the long
     * @return the value
     */
    public static ValueLong get(long i) {
        if (i >= 0 && i < STATIC_SIZE) {
            return STATIC_CACHE[(int) i];
        }
        return (ValueLong) Value.cache(new ValueLong(i));
    }

    @Override
    public Value add(Value v) {
        ValueLong other = (ValueLong) v;
        long result = value + other.value;
        int sv = Long.signum(value);
        int so = Long.signum(other.value);
        int sr = Long.signum(result);
        // if the operands have different signs overflow can not occur
        // if the operands have the same sign,
        // and the result has a different sign, then it is an overflow
        // it can not be an overflow when one of the operands is 0
        if (sv != so || sr == so || sv == 0 || so == 0) {
            return ValueLong.get(result);
        }
        throw getOverflow();
    }

    @Override
    public int getSignum() {
        return Long.signum(value);
    }

    @Override
    public Value negate() {
        if (value == Long.MIN_VALUE) {
            throw getOverflow();
        }
        return ValueLong.get(-value);
    }

    private DbException getOverflow() {
        return DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
                Long.toString(value));
    }

    @Override
    public Value subtract(Value v) {
        ValueLong other = (ValueLong) v;
        int sv = Long.signum(value);
        int so = Long.signum(other.value);
        // if the operands have the same sign, then overflow can not occur
        // if the second operand is 0, then overflow can not occur
        if (sv == so || so == 0) {
            return ValueLong.get(value - other.value);
        }
        // now, if the other value is Long.MIN_VALUE, it must be an overflow
        // x - Long.MIN_VALUE overflows for x>=0
        return add(other.negate());
    }

    @Override
    public Value multiply(Value v) {
        ValueLong other = (ValueLong) v;
        long result = value * other.value;
        if (value == 0 || value == 1 || other.value == 0 || other.value == 1) {
            return ValueLong.get(result);
        }
        if (isInteger(value) && isInteger(other.value)) {
            return ValueLong.get(result);
        }
        // just checking one case is not enough: Long.MIN_VALUE * -1
        // probably this is correct but I'm not sure
        // if (result / value == other.value && result / other.value == value) {
        //    return ValueLong.get(result);
        //}
        BigInteger bv = BigInteger.valueOf(value);
        BigInteger bo = BigInteger.valueOf(other.value);
        BigInteger br = bv.multiply(bo);
        if (br.compareTo(MIN) < 0 || br.compareTo(MAX) > 0) {
            throw getOverflow();
        }
        return ValueLong.get(br.longValue());
    }

    @Override
    public Value divide(Value v) {
        ValueLong other = (ValueLong) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueLong.get(value / other.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueLong other = (ValueLong) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueLong.get(this.value % other.value);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getType() {
        return Value.LONG;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueLong v = (ValueLong) o;
        return MathUtils.compareLong(value, v.value);
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
        return (int) (value ^ (value >> 32));
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setLong(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLong && value == ((ValueLong) other).value;
    }

}
