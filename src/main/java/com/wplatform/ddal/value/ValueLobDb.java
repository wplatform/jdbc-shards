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

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.*;

/**
 * A implementation of the BLOB and CLOB data types.
 * <p>
 * Small objects are kept in memory and stored in the record.
 * Large objects are either stored in the database, or in temporary files.
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ValueLobDb extends Value implements Value.ValueClob,
        Value.ValueBlob {

    private final int type;
    private final long lobId;
    private final byte[] small;
    /*private final DataHandler handler;*/

    /**
     * For a BLOB, precision is length in bytes.
     * For a CLOB, precision is length in chars.
     */
    private final long precision;

    private final String fileName;
    private int tableId;
    private int hash;

    private ValueLobDb(int type, int tableId, long lobId, long precision) {
        this.type = type;
        this.tableId = tableId;
        this.lobId = lobId;
        this.precision = precision;
        this.small = null;
        this.fileName = null;
    }

    private ValueLobDb(int type, byte[] small, long precision) {
        this.type = type;
        this.small = small;
        this.precision = precision;
        this.lobId = 0;
        /*this.handler = null;*/
        this.fileName = null;
    }

    /**
     * Create a CLOB in a temporary file.
     */
    private ValueLobDb(Reader in, long remaining)
            throws IOException {
        this.type = Value.CLOB;
        this.small = null;
        this.lobId = 0;
        this.fileName = createTempLobFileName();
        long tmpPrecision = 0;
        try {
            char[] buff = new char[Constants.IO_BUFFER_SIZE];
            while (true) {
                int len = getBufferSize(remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
            }
        } finally {
            //tempFile.close();
        }
        this.precision = tmpPrecision;
    }

    /**
     * Create a BLOB in a temporary file.
     */
    private ValueLobDb(byte[] buff, int len, InputStream in,
                       long remaining) throws IOException {
        this.type = Value.BLOB;
        this.small = null;
        this.lobId = 0;
        this.fileName = createTempLobFileName();
        RandomAccessFile tempFile = new RandomAccessFile(fileName, "rw");
        long tmpPrecision = 0;
        try {
            while (true) {
                tmpPrecision += len;
                tempFile.write(buff, 0, len);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len <= 0) {
                    break;
                }
            }
        } finally {
            tempFile.close();
        }
        this.precision = tmpPrecision;
    }

    private static String createTempLobFileName()
            throws IOException {
        String path = getDatabasePath();
        if (path.length() == 0) {
            path = SysProperties.PREFIX_TEMP_FILE;
        }
        return FileUtils.createTempFile(path, Constants.SUFFIX_TEMP_FILE, true, true);
    }

    static String getDatabasePath() {
        return "";
    }

    /**
     * Create a LOB value.
     *
     * @param type      the type
     * @param handler   the data handler
     * @param tableId   the table id
     * @param id        the lob id
     * @param hmac      the message authentication code
     * @param precision the precision (number of bytes / characters)
     * @return the value
     */
    public static ValueLobDb create(int type,
                                    int tableId, long id, long precision) {
        return new ValueLobDb(type, tableId, id, precision);
    }

    /**
     * Create a temporary CLOB value from a stream.
     *
     * @param in      the reader
     * @param length  the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLobDb createTempClob(Reader in, long length) {
        BufferedReader reader;
        if (in instanceof BufferedReader) {
            reader = (BufferedReader) in;
        } else {
            reader = new BufferedReader(in, Constants.IO_BUFFER_SIZE);
        }
        try {
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(reader, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                reader.mark(len);
                len = IOUtils.readFully(reader, buff, len);
            }
            if (len <= getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(Constants.UTF8);
                return ValueLobDb.createSmallLob(Value.CLOB, small, len);
            }
            reader.reset();
            ValueLobDb lob = new ValueLobDb(reader, remaining);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    /**
     * Create a temporary BLOB value from a stream.
     *
     * @param in      the input stream
     * @param length  the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    public static ValueLobDb createTempBlob(InputStream in, long length) {
        try {
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(remaining);
            byte[] buff;
            if (len >= Integer.MAX_VALUE) {
                buff = IOUtils.readBytesAndClose(in, -1);
                len = buff.length;
            } else {
                buff = DataUtils.newBytes(len);
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= getMaxLengthInplaceLob()) {
                byte[] small = DataUtils.newBytes(len);
                System.arraycopy(buff, 0, small, 0, len);
                return ValueLobDb.createSmallLob(Value.BLOB, small, small.length);
            }
            ValueLobDb lob = new ValueLobDb(buff, len, in, remaining);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private static int getBufferSize(
            long remaining) {
        if (remaining < 0 || remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        int inplace = /*handler.*/getMaxLengthInplaceLob();
        long m = Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic because
            // inplace could be Integer.MAX_VALUE
            m = Math.min(remaining, inplace + 1L);
            // the buffer size must be bigger than the inplace lob, otherwise we
            // can't know if it must be stored in-place or not
            m = MathUtils.roundUpLong(m, Constants.IO_BUFFER_SIZE);
        }
        m = Math.min(remaining, m);
        m = MathUtils.convertLongToInt(m);
        if (m < 0) {
            m = Integer.MAX_VALUE;
        }
        return (int) m;
    }

    static int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type  the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the LOB
     */
    public static Value createSmallLob(int type, byte[] small) {
        int precision;
        if (type == Value.CLOB) {
            precision = new String(small, Constants.UTF8).length();
        } else {
            precision = small.length;
        }
        return createSmallLob(type, small, precision);
    }

    /**
     * Create a LOB object that fits in memory.
     *
     * @param type      the type (Value.BLOB or CLOB)
     * @param small     the byte array
     * @param precision the precision
     * @return the LOB
     */
    public static ValueLobDb createSmallLob(int type, byte[] small,
                                            long precision) {
        return new ValueLobDb(type, small, precision);
    }

    /**
     * Convert a lob to another data type. The data is fully read in memory
     * except when converting to BLOB or CLOB.
     *
     * @param t the new type
     * @return the converted value
     */
    @Override
    public Value convertTo(int t) {
        if (t == type) {
            return this;
        } else if (t == Value.CLOB) {
            if (small != null) {
                return ValueLobDb.createSmallLob(t, small);
            } else {
                return ValueLobDb.createTempClob(getReader(), -1);
            }
        } else if (t == Value.BLOB) {
            if (small != null) {
                return ValueLobDb.createSmallLob(t, small);
            } else {
                return ValueLobDb.createTempBlob(getInputStream(), -1);
            }
        }
        return super.convertTo(t);
    }

    public boolean isStored() {
        return small == null && fileName == null;
    }

    @Override
    public void close() {
        if (fileName != null) {
            FileUtils.delete(fileName);
        }
    }

    /**
     * Get the current table id of this lob.
     *
     * @return the table id
     */
    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public String getString() {
        int len = precision > Integer.MAX_VALUE || precision == 0 ?
                Integer.MAX_VALUE : (int) precision;
        try {
            if (type == Value.CLOB) {
                if (small != null) {
                    return new String(small, Constants.UTF8);
                }
                return IOUtils.readStringAndClose(getReader(), len);
            }
            byte[] buff;
            if (small != null) {
                buff = small;
            } else {
                buff = IOUtils.readBytesAndClose(getInputStream(), len);
            }
            return StringUtils.convertBytesToHex(buff);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public byte[] getBytes() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytes();
        }
        byte[] data = getBytesNoCopy();
        return Utils.cloneByteArray(data);
    }

    @Override
    public byte[] getBytesNoCopy() {
        if (type == CLOB) {
            // convert hex to string
            return super.getBytesNoCopy();
        }
        if (small != null) {
            return small;
        }
        try {
            return IOUtils.readBytesAndClose(getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, toString());
        }
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            if (precision > 4096) {
                // TODO: should calculate the hash code when saving, and store
                // it in the database file
                return (int) (precision ^ (precision >>> 32));
            }
            if (type == CLOB) {
                hash = getString().hashCode();
            } else {
                hash = Utils.getByteArrayHash(getBytes());
            }
        }
        return hash;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        if (v instanceof ValueLobDb) {
            ValueLobDb v2 = (ValueLobDb) v;
            if (v == this) {
                return 0;
            }
            if (lobId == v2.lobId && small == null && v2.small == null) {
                return 0;
            }
        }
        if (type == Value.CLOB) {
            return Integer.signum(getString().compareTo(v.getString()));
        }
        byte[] v2 = v.getBytesNoCopy();
        return Utils.compareNotNullSigned(getBytes(), v2);
    }

    @Override
    public Object getObject() {
        if (type == Value.CLOB) {
            return getReader();
        }
        return getInputStream();
    }

    @Override
    public Reader getReader() {
        return IOUtils.getBufferedReader(getInputStream());
    }

    @Override
    public InputStream getInputStream() {
        if (small != null) {
            return new ByteArrayInputStream(small);
        } else if (fileName != null) {
            try {
                return new FileInputStream(fileName);
            } catch (FileNotFoundException e) {
                throw DbException.convert(e);
            }
        }
        String typeName = type == Value.CLOB ? "CLOB" : "BLOB";
        throw DbException.throwInternalError(typeName + " data error!");
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        long p = getPrecision();
        if (p > Integer.MAX_VALUE || p <= 0) {
            p = -1;
        }
        if (type == Value.BLOB) {
            prep.setBinaryStream(parameterIndex, getInputStream(), (int) p);
        } else {
            prep.setCharacterStream(parameterIndex, getReader(), (int) p);
        }
    }

    @Override
    public String getSQL() {
        String s;
        if (type == Value.CLOB) {
            s = getString();
            return StringUtils.quoteStringSQL(s);
        }
        byte[] buff = getBytes();
        s = StringUtils.convertBytesToHex(buff);
        return "X'" + s + "'";
    }

    @Override
    public String getTraceSQL() {
        if (small != null && getPrecision() <= SysProperties.MAX_TRACE_DATA_LENGTH) {
            return getSQL();
        }
        StringBuilder buff = new StringBuilder();
        if (type == Value.CLOB) {
            buff.append("SPACE(").append(getPrecision());
        } else {
            buff.append("CAST(REPEAT('00', ").append(getPrecision()).append(") AS BINARY");
        }
        buff.append(" /* table: ").append(tableId).append(" id: ")
                .append(lobId).append(" */)");
        return buff.toString();
    }

    /**
     * Get the data if this a small lob value.
     *
     * @return the data
     */
    @Override
    public byte[] getSmall() {
        return small;
    }

    @Override
    public int getDisplaySize() {
        return MathUtils.convertLongToInt(getPrecision());
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueLobDb && compareSecure((Value) other, null) == 0;
    }

    @Override
    public int getMemory() {
        if (small != null) {
            return small.length + 104;
        }
        return 140;
    }

    /**
     * Create an independent copy of this temporary value.
     * The file will not be deleted automatically.
     *
     * @return the value
     */
    @Override
    public ValueLobDb copyToTemp() {
        return this;
    }

    public long getLobId() {
        return lobId;
    }

    @Override
    public String toString() {
        return "lob: " + fileName + " table: " + tableId + " id: " + lobId;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLobDb lob;
        if (type == CLOB) {
            lob = ValueLobDb.createTempClob(getReader(), precision);
        } else {
            lob = ValueLobDb.createTempBlob(getInputStream(), precision);
        }
        return lob;
    }

}
