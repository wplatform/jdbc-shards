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
package com.wplatform.ddal.dbobject.table;

import com.wplatform.ddal.result.SortOrder;

/**
 * This represents a column item of an index. This is required because some
 * indexes support descending sorted columns.
 */
public class IndexColumn {

    /**
     * The column name.
     */
    public String columnName;

    /**
     * The column, or null if not set.
     */
    public Column column;

    /**
     * The sort type. Ascending (the default) and descending are supported;
     * nulls can be sorted first or last.
     */
    public int sortType = SortOrder.ASCENDING;

    /**
     * Create an array of index columns from a list of columns. The default sort
     * type is used.
     *
     * @param columns the column list
     * @return the index column array
     */
    public static IndexColumn[] wrap(Column[] columns) {
        IndexColumn[] list = new IndexColumn[columns.length];
        for (int i = 0; i < list.length; i++) {
            list[i] = new IndexColumn();
            list[i].column = columns[i];
        }
        return list;
    }

    /**
     * Map the columns using the column names and the specified table.
     *
     * @param indexColumns the column list with column names set
     * @param table        the table from where to map the column names to columns
     */
    public static void mapColumns(IndexColumn[] indexColumns, Table table) {
        for (IndexColumn col : indexColumns) {
            col.column = table.getColumn(col.columnName);
        }
    }

    /**
     * Get the SQL snippet for this index column.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        StringBuilder buff = new StringBuilder(column.getSQL());
        if ((sortType & SortOrder.DESCENDING) != 0) {
            buff.append(" DESC");
        }
        if ((sortType & SortOrder.NULLS_FIRST) != 0) {
            buff.append(" NULLS FIRST");
        } else if ((sortType & SortOrder.NULLS_LAST) != 0) {
            buff.append(" NULLS LAST");
        }
        return buff.toString();
    }
}
