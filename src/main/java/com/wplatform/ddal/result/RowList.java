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

import java.util.ArrayList;

import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.New;

/**
 * A list of rows. If the list grows too large, it is buffered to disk
 * automatically.
 */
public class RowList {

    private final Session session;
    private final ArrayList<Row> list = New.arrayList();
    private final int maxMemory;
    private int size;
    private int index;
    private int memory;

    /**
     * Construct a new row list for this session.
     *
     * @param session the session
     */
    public RowList(Session session) {
        this.session = session;
        maxMemory = session.getDatabase().getMaxOperationMemory();
    }


    private void writeAllRows() {
        session.getId();
        throw DbException.getUnsupportedException("TODO");
    }

    /**
     * Add a row to the list.
     *
     * @param r the row to add
     */
    public void add(Row r) {
        list.add(r);
        memory += r.getMemory() + Constants.MEMORY_POINTER;
        if (maxMemory > 0 && memory > maxMemory) {
            writeAllRows();
        }
        size++;
    }

    /**
     * Remove all rows from the list.
     */
    public void reset() {
        index = 0;
    }

    /**
     * Check if there are more rows in this list.
     *
     * @return true it there are more rows
     */
    public boolean hasNext() {
        return index < size;
    }

    /**
     * Get the next row from the list.
     *
     * @return the next row
     */
    public Row next() {
        Row r = list.get(index++);
        return r;
    }

    /**
     * Get the number of rows in this list.
     *
     * @return the number of rows
     */
    public int size() {
        return size;
    }

    /**
     * Close the result list and delete the temporary file.
     */
    public void close() {

    }

}
