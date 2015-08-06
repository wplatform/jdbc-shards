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

import com.wplatform.ddal.value.Value;

/**
 * This interface is used to extend the LocalResult class, if data does not fit
 * in memory.
 */
public interface ResultExternal {

    /**
     * Reset the current position of this object.
     */
    void reset();

    /**
     * Get the next row from the result.
     *
     * @return the next row or null
     */
    Value[] next();

    /**
     * Add a row to this object.
     *
     * @param values the row to add
     * @return the new number of rows in this object
     */
    int addRow(Value[] values);

    /**
     * Add a number of rows to the result.
     *
     * @param rows the list of rows to add
     * @return the new number of rows in this object
     */
    int addRows(ArrayList<Value[]> rows);

    /**
     * This method is called after all rows have been added.
     */
    void done();

    /**
     * Close this object and delete the temporary file.
     */
    void close();

    /**
     * Remove the row with the given values from this object if such a row
     * exists.
     *
     * @param values the row
     * @return the new row count
     */
    int removeRow(Value[] values);

    /**
     * Check if the given row exists in this object.
     *
     * @param values the row
     * @return true if it exists
     */
    boolean contains(Value[] values);

    /**
     * Create a shallow copy of this object if possible.
     *
     * @return the shallow copy, or null
     */
    ResultExternal createShallowCopy();

}
