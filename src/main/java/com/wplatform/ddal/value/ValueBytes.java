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
import java.util.Arrays;

import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.util.MathUtils;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.util.Utils;

/**
 * Implementation of the BINARY data type.
 * It is also the base class for ValueJavaObject.
 */
public class ValueBytes extends Value {

    private static final ValueBytes EMPTY = new ValueBytes(Utils.EMPTY_BYTES);

    /**
     * The value.
     */
    protected byte[] value;

    /**
     * The hash code.
     */
    protected int hash;

    protected ValueBytes(byte[] v) {
        this.value = v;
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Clone the data.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes get(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        b = Utils.cloneByteArray(b);
        return getNoCopy(b);
    }

    /**
     * Get or create a bytes value for the given byte array.
     * Do not clone the date.
     *
     * @param b the byte array
     * @return the value
     */
    public static ValueBytes getNoCopy(byte[] b) {
        if (b.length == 0) {
            return EMPTY;
        }
        ValueBytes obj = new ValueBytes(b);
        if (b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueBytes) Value.cache(obj);
    }

    @Override
    public int getType() {
        return Value.BYTES;
    }

    @Override
    public String getSQL() {
        return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'";
    }

    @Override
    public byte[] getBytesNoCopy() {
        return value;
    }

    @Override
    public byte[] getBytes() {
        return Utils.cloneByteArray(getBytesNoCopy());
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        byte[] v2 = ((ValueBytes) v).value;
        if (mode.isBinaryUnsigned()) {
            return Utils.compareNotNullUnsigned(value, v2);
        }
        return Utils.compareNotNullSigned(value, v2);
    }

    @Override
    public String getString() {
        return StringUtils.convertBytesToHex(value);
    }

    @Override
    public long getPrecision() {
        return value.length;
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            hash = Utils.getByteArrayHash(value);
        }
        return hash;
    }

    @Override
    public Object getObject() {
        return getBytes();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        prep.setBytes(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return MathUtils.convertLongToInt(value.length * 2L);
    }

    @Override
    public int getMemory() {
        return value.length + 24;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueBytes
                && Arrays.equals(value, ((ValueBytes) other).value);
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (value.length <= precision) {
            return this;
        }
        int len = MathUtils.convertLongToInt(precision);
        byte[] buff = new byte[len];
        System.arraycopy(value, 0, buff, 0, len);
        return get(buff);
    }

}
