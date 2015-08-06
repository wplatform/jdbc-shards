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
 * Implementation of the BLOB and CLOB data types. Small objects are kept in
 * memory and stored in the record.
 * <p>
 * Large objects are stored in their own files. When large objects are set in a
 * prepared statement, they are first stored as 'temporary' files. Later, when
 * they are used in a record, and when the record is stored, the lob files are
 * linked: the file is renamed using the file format (tableId).(objectId). There
 * is one exception: large variables are stored in the file (-1).(objectId).
 * <p>
 * When lobs are deleted, they are first renamed to a temp file, and if the
 * delete operation is committed the file is deleted.
 * <p>
 * Data compression is supported.
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ValueLob extends Value {

    private static final SmallLRUCache<String, String[]> cache = SmallLRUCache.newInstance(128);
    /**
     * This counter is used to calculate the next directory to store lobs. It is
     * better than using a random number because less directories are created.
     */
    private static int dirCounter;

    private final int type;
    private long precision;
    private int tableId;
    private int objectId;
    private String fileName;
    private boolean linked;
    private byte[] small;
    private int hash;

    private ValueLob(int type, String fileName,
                     int tableId, int objectId, boolean linked, long precision) {
        this.type = type;
        this.fileName = fileName;
        this.tableId = tableId;
        this.objectId = objectId;
        this.linked = linked;
        this.precision = precision;
    }

    private ValueLob(int type, byte[] small) {
        this.type = type;
        this.small = small;
        if (small != null) {
            if (type == Value.BLOB) {
                this.precision = small.length;
            } else {
                this.precision = getString().length();
            }
        }
    }

    private static ValueLob copy(ValueLob lob) {
        ValueLob copy = new ValueLob(lob.type, lob.fileName,
                lob.tableId, lob.objectId, lob.linked, lob.precision);
        copy.small = lob.small;
        return copy;
    }

    /**
     * Create a small lob using the given byte array.
     *
     * @param type  the type (Value.BLOB or CLOB)
     * @param small the byte array
     * @return the lob value
     */
    private static ValueLob createSmallLob(int type, byte[] small) {
        return new ValueLob(type, small);
    }

    private static String getFileName(int tableId,
                                      int objectId) {
        if (SysProperties.CHECK && tableId == 0 && objectId == 0) {
            DbException.throwInternalError("0 LOB");
        }
        String table = tableId < 0 ? ".temp" : ".t" + tableId;
        return getFileNamePrefix(getDatabasePath(), objectId) +
                table + Constants.SUFFIX_LOB_FILE;
    }

    private static String getDatabasePath() {
        return new File(Utils.getProperty("java.io.tmpdir", "."),
                SysProperties.PREFIX_TEMP_FILE).getAbsolutePath();
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type        the data type
     * @param handler     the file handler
     * @param tableId     the table object id
     * @param objectId    the object id
     * @param precision   the precision (length in elements)
     * @param compression if compression is used
     * @return the value object
     */
    public static ValueLob openLinked(int type,
                                      int tableId, int objectId, long precision) {
        String fileName = getFileName(tableId, objectId);
        return new ValueLob(type, fileName, tableId, objectId,
                true/* linked */, precision);
    }

    /**
     * Create a LOB value with the given parameters.
     *
     * @param type        the data type
     * @param handler     the file handler
     * @param tableId     the table object id
     * @param objectId    the object id
     * @param precision   the precision (length in elements)
     * @param compression if compression is used
     * @param fileName    the file name
     * @return the value object
     */
    public static ValueLob openUnlinked(int type,
                                        int tableId, int objectId, long precision,
                                        String fileName) {
        return new ValueLob(type, fileName, tableId, objectId,
                false/* linked */, precision);
    }

    /**
     * Create a CLOB value from a stream.
     *
     * @param in      the reader
     * @param length  the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    private static ValueLob createClob(Reader in, long length) {
        try {
            long remaining = Long.MAX_VALUE;
            if (length >= 0 && length < remaining) {
                remaining = length;
            }
            int len = getBufferSize(remaining);
            char[] buff;
            if (len >= Integer.MAX_VALUE) {
                String data = IOUtils.readStringAndClose(in, -1);
                buff = data.toCharArray();
                len = buff.length;
            } else {
                buff = new char[len];
                len = IOUtils.readFully(in, buff, len);
            }
            if (len <= getMaxLengthInplaceLob()) {
                byte[] small = new String(buff, 0, len).getBytes(Constants.UTF8);
                return ValueLob.createSmallLob(Value.CLOB, small);
            }
            ValueLob lob = new ValueLob(Value.CLOB, null);
            lob.createFromReader(buff, len, in, remaining);
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
        int inplace = getMaxLengthInplaceLob();
        long m = Constants.IO_BUFFER_SIZE;
        if (m < remaining && m <= inplace) {
            // using "1L" to force long arithmetic
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

    private static int getMaxLengthInplaceLob() {
        return SysProperties.LOB_CLIENT_MAX_SIZE_MEMORY;
    }

    private static String getFileNamePrefix(String path, int objectId) {
        String name;
        int f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
        if (f > 0) {
            name = SysProperties.FILE_SEPARATOR + objectId;
        } else {
            name = "";
        }
        objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        while (objectId > 0) {
            f = objectId % SysProperties.LOB_FILES_PER_DIRECTORY;
            name = SysProperties.FILE_SEPARATOR + f +
                    Constants.SUFFIX_LOBS_DIRECTORY + name;
            objectId /= SysProperties.LOB_FILES_PER_DIRECTORY;
        }
        name = FileUtils.toRealPath(path +
                Constants.SUFFIX_LOBS_DIRECTORY + name);
        return name;
    }

    private static int getNewObjectId() {
        String path = getDatabasePath();
        int newId = 0;
        int lobsPerDir = SysProperties.LOB_FILES_PER_DIRECTORY;
        while (true) {
            String dir = getFileNamePrefix(path, newId);
            String[] list = getFileList(dir);
            int fileCount = 0;
            boolean[] used = new boolean[lobsPerDir];
            for (String name : list) {
                if (name.endsWith(Constants.SUFFIX_DB_FILE)) {
                    name = FileUtils.getName(name);
                    String n = name.substring(0, name.indexOf('.'));
                    int id;
                    try {
                        id = Integer.parseInt(n);
                    } catch (NumberFormatException e) {
                        id = -1;
                    }
                    if (id > 0) {
                        fileCount++;
                        used[id % lobsPerDir] = true;
                    }
                }
            }
            int fileId = -1;
            if (fileCount < lobsPerDir) {
                for (int i = 1; i < lobsPerDir; i++) {
                    if (!used[i]) {
                        fileId = i;
                        break;
                    }
                }
            }
            if (fileId > 0) {
                newId += fileId;
                invalidateFileList(dir);
                break;
            }
            if (newId > Integer.MAX_VALUE / lobsPerDir) {
                // this directory path is full: start from zero
                newId = 0;
                dirCounter = MathUtils.randomInt(lobsPerDir - 1) * lobsPerDir;
            } else {
                // calculate the directory.
                // start with 1 (otherwise we don't know the number of
                // directories).
                // it doesn't really matter what directory is used, it might as
                // well be random (but that would generate more directories):
                // int dirId = RandomUtils.nextInt(lobsPerDir - 1) + 1;
                int dirId = (dirCounter++ / (lobsPerDir - 1)) + 1;
                newId = newId * lobsPerDir;
                newId += dirId * lobsPerDir;
            }
        }
        return newId;
    }

    private static void invalidateFileList(String dir) {
        if (cache != null) {
            synchronized (cache) {
                cache.remove(dir);
            }
        }
    }

    private static String[] getFileList(String dir) {
        String[] list;
        if (cache == null) {
            list = FileUtils.newDirectoryStream(dir).toArray(new String[0]);
        } else {
            synchronized (cache) {
                list = cache.get(dir);
                if (list == null) {
                    list = FileUtils.newDirectoryStream(dir).toArray(new String[0]);
                    cache.put(dir, list);
                }
            }
        }
        return list;
    }

    /**
     * Create a BLOB value from a stream.
     *
     * @param in      the input stream
     * @param length  the number of characters to read, or -1 for no limit
     * @param handler the data handler
     * @return the lob value
     */
    private static ValueLob createBlob(InputStream in, long length) {
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
                return ValueLob.createSmallLob(Value.BLOB, small);
            }
            ValueLob lob = new ValueLob(Value.BLOB, null);
            lob.createFromStream(buff, len, in, remaining);
            return lob;
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private static synchronized void deleteFile(
            String fileName) {
        FileUtils.delete(fileName);
    }

    private static synchronized void renameFile(
            String oldName, String newName) {
        FileUtils.move(oldName, newName);
    }

    private static void copyFileTo(String sourceFileName,
                                   String targetFileName) {
        try {
            IOUtils.copyFiles(sourceFileName, targetFileName);
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
    }

    private void createFromReader(char[] buff, int len, Reader in,
                                  long remaining) throws IOException {
        FileOutputStream out = initLarge();
        try {
            while (true) {
                precision += len;
                byte[] b = new String(buff, 0, len).getBytes(Constants.UTF8);
                out.write(b, 0, b.length);
                remaining -= len;
                if (remaining <= 0) {
                    break;
                }
                len = getBufferSize(remaining);
                len = IOUtils.readFully(in, buff, len);
                if (len == 0) {
                    break;
                }
            }
        } finally {
            out.close();
        }
    }

    private FileOutputStream initLarge() {
        this.tableId = 0;
        this.linked = false;
        this.precision = 0;
        this.small = null;

        String path = getDatabasePath();
        objectId = getNewObjectId();
        fileName = getFileNamePrefix(path, objectId) + Constants.SUFFIX_TEMP_FILE;
        FileOutputStream out;
        try {
            out = new FileOutputStream(fileName);
        } catch (FileNotFoundException e) {
            throw DbException.convert(e);
        }
        return out;
    }

    private void createFromStream(byte[] buff, int len, InputStream in,
                                  long remaining) throws IOException {
        FileOutputStream out = initLarge();
        try {
            while (true) {
                precision += len;
                out.write(buff, 0, len);
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
            out.close();
        }
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
            ValueLob copy = ValueLob.createClob(getReader(), -1);
            return copy;
        } else if (t == Value.BLOB) {
            ValueLob copy = ValueLob.createBlob(getInputStream(), -1);
            return copy;
        }
        return super.convertTo(t);
    }

    @Override
    public boolean isLinked() {
        return linked;
    }

    /**
     * Get the current file name where the lob is saved.
     *
     * @return the file name or null
     */
    public String getFileName() {
        return fileName;
    }

    @Override
    public void close() {
        if (fileName != null) {
            FileUtils.delete(fileName);
        }
    }

    @Override
    public void unlink() {
        if (linked && fileName != null) {
            String temp;
            // synchronize on the database, to avoid concurrent temp file
            // creation / deletion / backup

            temp = getFileName(-1, objectId);
            deleteFile(temp);
            renameFile(fileName, temp);
            fileName = temp;
            linked = false;

        }
    }

    @Override
    public Value link(int tabId) {
        if (fileName == null) {
            this.tableId = tabId;
            return this;
        }
        if (linked) {
            ValueLob copy = ValueLob.copy(this);
            copy.objectId = getNewObjectId();
            copy.tableId = tabId;
            String live = getFileName(copy.tableId, copy.objectId);
            copyFileTo(fileName, live);
            copy.fileName = live;
            copy.linked = true;
            return copy;
        }
        if (!linked) {
            this.tableId = tabId;
            String live = getFileName(tableId, objectId);
            renameFile(fileName, live);
            fileName = live;
            linked = true;
        }
        return this;
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

    /**
     * Get the current object id of this lob.
     *
     * @return the object id
     */
    public int getObjectId() {
        return objectId;
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
            throw DbException.convertIOException(e, fileName);
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
            return IOUtils.readBytesAndClose(
                    getInputStream(), Integer.MAX_VALUE);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
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
        if (fileName == null) {
            return new ByteArrayInputStream(small);
        }
        try {
            return new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            String typeName = type == Value.CLOB ? "CLOB" : "BLOB";
            throw DbException.throwInternalError(typeName + " data error!");
        }
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
        buff.append(" /* ").append(fileName).append(" */)");
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
        return other instanceof ValueLob && compareSecure((Value) other, null) == 0;
    }

    /**
     * Store the lob data to a file if the size of the buffer is larger than the
     * maximum size for an in-place lob.
     *
     * @param h the data handler
     */
    public void convertToFileIfRequired() {
        try {
            if (small != null && small.length > getMaxLengthInplaceLob()) {
                int len = getBufferSize(Long.MAX_VALUE);
                int tabId = tableId;
                if (type == Value.BLOB) {
                    createFromStream(
                            DataUtils.newBytes(len), 0, getInputStream(), Long.MAX_VALUE);
                } else {
                    createFromReader(
                            new char[len], 0, getReader(), Long.MAX_VALUE);
                }
                Value v2 = link(tabId);
                if (SysProperties.CHECK && v2 != this) {
                    DbException.throwInternalError();
                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        }
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
    public ValueLob copyToTemp() {
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision);
        }
        return lob;
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (this.precision <= precision) {
            return this;
        }
        ValueLob lob;
        if (type == CLOB) {
            lob = ValueLob.createClob(getReader(), precision);
        } else {
            lob = ValueLob.createBlob(getInputStream(), precision);
        }
        return lob;
    }

}
