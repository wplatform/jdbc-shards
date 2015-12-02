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

/**
 * Implementation of the DOUBLE data type.
 */
public class ValueDouble extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 17;

    /**
     * The maximum display size of a double.
     * Example: -3.3333333333333334E-100
     */
    public static final int DISPLAY_SIZE = 24;

    /**
     * Double.doubleToLongBits(0.0)
     */
    public static final long ZERO_BITS = Double.doubleToLongBits(0.0);

    private static final ValueDouble ZERO = new ValueDouble(0.0);
    private static final ValueDouble ONE = new ValueDouble(1.0);
    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
        this.value = value;
    }

    /**
     * Get or create double value for the given double.
     *
     * @param d the double
     * @return the value
     */
    public static ValueDouble get(double d) {
        if (d == 1.0) {
            return ONE;
        } else if (d == 0.0) {
            // unfortunately, -0.0 == 0.0, but we don't want to return
            // 0.0 in this case
            if (Double.doubleToLongBits(d) == ZERO_BITS) {
                return ZERO;
            }
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return (ValueDouble) Value.cache(new ValueDouble(d));
    }

    @Override
    public Value add(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value + v2.value);
    }

    @Override
    public Value subtract(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value - v2.value);
    }

    @Override
    public Value negate() {
        return ValueDouble.get(-value);
    }

    @Override
    public Value multiply(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value * v2.value);
    }

    @Override
    public Value divide(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        if (v2.value == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value / v2.value);
    }

    @Override
    public ValueDouble modulus(Value v) {
        ValueDouble other = (ValueDouble) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value % other.value);
    }

    @Override
    public String getSQL() {
        if (value == Double.POSITIVE_INFINITY) {
            return "POWER(0, -1)";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "(-POWER(0, -1))";
        } else if (Double.isNaN(value)) {
            return "SQRT(-1)";
        }
        String s = getString();
        if (s.equals("-0.0")) {
            return "-CAST(0 AS DOUBLE)";
        }
        return s;
    }

    @Override
    public int getType() {
        return Value.DOUBLE;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueDouble v = (ValueDouble) o;
        return Double.compare(value, v.value);
    }

    @Override
    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    @Override
    public double getDouble() {
        return value;
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
    public int getScale() {
        return 0;
    }

    @Override
    public int hashCode() {
        long hash = Double.doubleToLongBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    @Override
    public Object getObject() {
        return Double.valueOf(value);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setDouble(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueDouble)) {
            return false;
        }
        return compareSecure((ValueDouble) other, null) == 0;
    }

}
