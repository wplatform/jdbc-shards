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
package com.wplatform.ddal.result;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.Data;
import com.wplatform.ddal.util.FileUtils;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

/**
 * This class implements the disk buffer for the LocalResult class.
 */
class ResultDiskBuffer implements ResultExternal {


    private static final int READ_AHEAD = 128;

    private final Data rowBuff;
    private final ArrayList<ResultDiskTape> tapes;
    private final ResultDiskTape mainTape;
    private final SortOrder sort;
    private final int columnCount;
    private final int maxBufferSize;
    private final ResultDiskBuffer parent;
    private FileChannel file;
    private int rowCount;
    private boolean closed;
    private int childCount;

    private String fileName;

    ResultDiskBuffer(Session session, SortOrder sort, int columnCount) {
        this.parent = null;
        this.sort = sort;
        this.columnCount = columnCount;
        rowBuff = Data.create(Constants.DEFAULT_PAGE_SIZE);
        fileName = createTempFile();
        try {
            file = FileUtils.open(fileName, "rw");
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
        //file.seek(FileStore.HEADER_LENGTH);
        if (sort != null) {
            tapes = New.arrayList();
            mainTape = null;
        } else {
            tapes = null;
            mainTape = new ResultDiskTape();
            //mainTape.pos = FileStore.HEADER_LENGTH;
        }
        this.maxBufferSize = 4 * 1024;
    }

    private ResultDiskBuffer(ResultDiskBuffer parent) {
        this.parent = parent;
        rowBuff = Data.create(Constants.DEFAULT_PAGE_SIZE);
        file = parent.file;
        if (parent.tapes != null) {
            tapes = New.arrayList();
            for (ResultDiskTape t : parent.tapes) {
                ResultDiskTape t2 = new ResultDiskTape();
                t2.pos = t2.start = t.start;
                t2.end = t.end;
                tapes.add(t2);
            }
        } else {
            tapes = null;
        }
        if (parent.mainTape != null) {
            mainTape = new ResultDiskTape();
            mainTape.pos = 0;//FileStore.HEADER_LENGTH;
            mainTape.start = parent.mainTape.start;
            mainTape.end = parent.mainTape.end;
        } else {
            mainTape = null;
        }
        sort = parent.sort;
        columnCount = parent.columnCount;
        maxBufferSize = parent.maxBufferSize;
    }

    public synchronized ResultDiskBuffer createShallowCopy() {
        if (closed || parent != null) {
            return null;
        }
        childCount++;
        return new ResultDiskBuffer(this);
    }

    public int addRows(ArrayList<Value[]> rows) {
        if (sort != null) {
            sort.sort(rows);
        }
        Data buff = rowBuff;
        long start = getFilePointer();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int bufferLen = 0;
        for (Value[] row : rows) {
            buff.reset();
            buff.writeInt(0);
            for (int j = 0; j < columnCount; j++) {
                Value v = row[j];
                buff.checkCapacity(Data.getValueLen(v));
                buff.writeValue(v);
            }
            buff.fillAligned();
            int len = buff.length();
            buff.setInt(0, len);
            if (maxBufferSize > 0) {
                buffer.write(buff.getBytes(), 0, len);
                bufferLen += len;
                if (bufferLen > maxBufferSize) {
                    byte[] data = buffer.toByteArray();
                    buffer.reset();
                    write(data, 0, data.length);
                    bufferLen = 0;
                }
            } else {
                write(buff.getBytes(), 0, len);
            }
        }
        if (bufferLen > 0) {
            byte[] data = buffer.toByteArray();
            write(data, 0, data.length);
        }
        if (sort != null) {
            ResultDiskTape tape = new ResultDiskTape();
            tape.start = start;
            tape.end = getFilePointer();
            tapes.add(tape);
        } else {
            mainTape.end = getFilePointer();
        }
        rowCount += rows.size();
        return rowCount;
    }

    public void done() {
        //file.seek(FileStore.HEADER_LENGTH);
        //file.autoDelete();
        seek(0);
    }

    public void reset() {
        if (sort != null) {
            for (ResultDiskTape tape : tapes) {
                tape.pos = tape.start;
                tape.buffer = New.arrayList();
            }
        } else {
            mainTape.pos = 0;//FileStore.HEADER_LENGTH;
            mainTape.buffer = New.arrayList();
        }
    }

    private void readRow(ResultDiskTape tape) {
        int min = Constants.FILE_BLOCK_SIZE;
        Data buff = rowBuff;
        buff.reset();
        readFully(buff.getBytes(), 0, min);
        int len = buff.readInt();
        buff.checkCapacity(len);
        if (len - min > 0) {
            readFully(buff.getBytes(), min, len - min);
        }
        tape.pos += len;
        Value[] row = new Value[columnCount];
        for (int k = 0; k < columnCount; k++) {
            row[k] = buff.readValue();
        }
        tape.buffer.add(row);
    }

    public Value[] next() {
        return sort != null ? nextSorted() : nextUnsorted();
    }

    private Value[] nextUnsorted() {
        seek(mainTape.pos);
        if (mainTape.buffer.size() == 0) {
            for (int j = 0; mainTape.pos < mainTape.end && j < READ_AHEAD; j++) {
                readRow(mainTape);
            }
        }
        Value[] row = mainTape.buffer.get(0);
        mainTape.buffer.remove(0);
        return row;
    }

    private Value[] nextSorted() {
        int next = -1;
        for (int i = 0, size = tapes.size(); i < size; i++) {
            ResultDiskTape tape = tapes.get(i);
            if (tape.buffer.size() == 0 && tape.pos < tape.end) {
                seek(tape.pos);
                for (int j = 0; tape.pos < tape.end && j < READ_AHEAD; j++) {
                    readRow(tape);
                }
            }
            if (tape.buffer.size() > 0) {
                if (next == -1) {
                    next = i;
                } else if (compareTapes(tape, tapes.get(next)) < 0) {
                    next = i;
                }
            }
        }
        ResultDiskTape t = tapes.get(next);
        Value[] row = t.buffer.get(0);
        t.buffer.remove(0);
        return row;
    }

    private int compareTapes(ResultDiskTape a, ResultDiskTape b) {
        Value[] va = a.buffer.get(0);
        Value[] vb = b.buffer.get(0);
        return sort.compare(va, vb);
    }

    private synchronized void closeChild() {
        if (--childCount == 0 && closed) {
            closeAndDeleteSilently();
            file = null;
        }
    }

    protected void finalize() {
        close();
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (parent != null) {
            parent.closeChild();
        } else if (file != null) {
            if (childCount == 0) {
                closeAndDeleteSilently();
                file = null;
            }
        }
    }

    public int removeRow(Value[] values) {
        throw DbException.throwInternalError();
    }

    public boolean contains(Value[] values) {
        throw DbException.throwInternalError();
    }

    public int addRow(Value[] values) {
        throw DbException.throwInternalError();
    }

    /**
     * Create a temporary file in the database folder.
     *
     * @return the file name
     */
    public String createTempFile() {
        try {
            String name = "TEMP_RESULT_SET_";
            return FileUtils.createTempFile(name,
                    Constants.SUFFIX_TEMP_FILE, true, true);
        } catch (IOException e) {
            throw DbException.convertIOException(e, e.getMessage());
        }
    }

    /**
     * Get the current location of the file pointer.
     *
     * @return the location
     */
    private long getFilePointer() {
        try {
            return file.position();
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Go to the specified file location.
     *
     * @param pos the location
     */
    private void seek(long pos) {
        if (SysProperties.CHECK &&
                pos % Constants.FILE_BLOCK_SIZE != 0) {
            DbException.throwInternalError(
                    "unaligned seek " + fileName + " pos " + pos);
        }
        try {
            file.position(pos);
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Write a number of bytes.
     *
     * @param b   the source buffer
     * @param off the offset
     * @param len the number of bytes to write
     */
    public void write(byte[] b, int off, int len) {
        if (SysProperties.CHECK && (len < 0 ||
                len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError(
                    "unaligned write " + fileName + " len " + len);
        }
        try {
            FileUtils.writeFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            closeFileSilently();
            throw DbException.convertIOException(e, fileName);
        }
    }

    /**
     * Read a number of bytes.
     *
     * @param b   the target buffer
     * @param off the offset
     * @param len the number of bytes to read
     */
    public void readFully(byte[] b, int off, int len) {
        if (SysProperties.CHECK &&
                (len < 0 || len % Constants.FILE_BLOCK_SIZE != 0)) {
            DbException.throwInternalError(
                    "unaligned read " + fileName + " len " + len);
        }
        try {
            FileUtils.readFully(file, ByteBuffer.wrap(b, off, len));
        } catch (IOException e) {
            throw DbException.convertIOException(e, fileName);
        }
    }

    private void closeFileSilently() {
        try {
            file.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * Close the file (ignoring exceptions) and delete the file.
     */
    public void closeAndDeleteSilently() {
        if (file != null) {
            closeFileSilently();
            try {
                FileUtils.tryDelete(fileName);
            } catch (Exception e) {
                // TODO log such errors?
            }
        }
    }

    /**
     * Represents a virtual disk tape for the merge sort algorithm.
     * Each virtual disk tape is a region of the temp file.
     */
    static class ResultDiskTape {

        /**
         * The start position of this tape in the file.
         */
        long start;

        /**
         * The end position of this tape in the file.
         */
        long end;

        /**
         * The current read position.
         */
        long pos;

        /**
         * A list of rows in the buffer.
         */
        ArrayList<Value[]> buffer = New.arrayList();
    }

}
