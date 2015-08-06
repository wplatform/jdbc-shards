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

import com.wplatform.ddal.value.Value;

/**
 * The result interface is used by the LocalResult and ResultRemote class.
 * A result may contain rows, or just an update count.
 */
public interface ResultInterface {

    /**
     * Go to the beginning of the result, that means
     * before the first row.
     */
    void reset();

    /**
     * Get the current row.
     *
     * @return the row
     */
    Value[] currentRow();

    /**
     * Go to the next row.
     *
     * @return true if a row exists
     */
    boolean next();

    /**
     * Get the current row id, starting with 0.
     * -1 is returned when next() was not called yet.
     *
     * @return the row id
     */
    int getRowId();

    /**
     * Get the number of visible columns.
     * More columns may exist internally for sorting or grouping.
     *
     * @return the number of columns
     */
    int getVisibleColumnCount();

    /**
     * Get the number of rows in this object.
     *
     * @return the number of rows
     */
    int getRowCount();

    /**
     * Check if this result set should be closed, for example because it is
     * buffered using a temporary file.
     *
     * @return true if close should be called.
     */
    boolean needToClose();

    /**
     * Close the result and delete any temporary files
     */
    void close();

    /**
     * Get the column alias name for the column.
     *
     * @param i the column number (starting with 0)
     * @return the alias name
     */
    String getAlias(int i);

    /**
     * Get the schema name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the schema name or null
     */
    String getSchemaName(int i);

    /**
     * Get the table name for the column, if one exists.
     *
     * @param i the column number (starting with 0)
     * @return the table name or null
     */
    String getTableName(int i);

    /**
     * Get the column name.
     *
     * @param i the column number (starting with 0)
     * @return the column name
     */
    String getColumnName(int i);

    /**
     * Get the column data type.
     *
     * @param i the column number (starting with 0)
     * @return the column data type
     */
    int getColumnType(int i);

    /**
     * Get the precision for this column.
     *
     * @param i the column number (starting with 0)
     * @return the precision
     */
    long getColumnPrecision(int i);

    /**
     * Get the scale for this column.
     *
     * @param i the column number (starting with 0)
     * @return the scale
     */
    int getColumnScale(int i);

    /**
     * Get the display size for this column.
     *
     * @param i the column number (starting with 0)
     * @return the display size
     */
    int getDisplaySize(int i);

    /**
     * Check if this is an auto-increment column.
     *
     * @param i the column number (starting with 0)
     * @return true for auto-increment columns
     */
    boolean isAutoIncrement(int i);

    /**
     * Check if this column is nullable.
     *
     * @param i the column number (starting with 0)
     * @return Column.NULLABLE_*
     */
    int getNullable(int i);

    /**
     * Get the current fetch size for this result set.
     *
     * @return the fetch size
     */
    int getFetchSize();

    /**
     * Set the fetch size for this result set.
     *
     * @param fetchSize the new fetch size
     */
    void setFetchSize(int fetchSize);

}
