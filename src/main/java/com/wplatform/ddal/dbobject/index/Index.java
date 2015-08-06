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
// Created on 2014年12月25日
// $Id$
package com.wplatform.ddal.dbobject.index;

import com.wplatform.ddal.dbobject.schema.SchemaObject;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.result.SortOrder;

/**
 * An index. Indexes are used to speed up searching data.
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public interface Index extends SchemaObject {
    /**
     * Estimate the cost to search for rows given the search mask.
     * There is one element per column in the search mask.
     * For possible search masks, see IndexCondition.
     *
     * @param session   the session
     * @param masks     per-column comparison bit masks, null means 'always false',
     *                  see constants in IndexCondition
     * @param filter    the table filter
     * @param sortOrder the sort order
     * @return the estimated cost
     */
    double getCost(Session session, int[] masks, TableFilter filter,
                   SortOrder sortOrder);

    /**
     * Get the index of a column in the list of index columns
     *
     * @param col the column
     * @return the index (0 meaning first column)
     */
    int getColumnIndex(Column col);

    /**
     * Get the indexed columns as index columns (with ordering information).
     *
     * @return the index columns
     */
    IndexColumn[] getIndexColumns();

    /**
     * Get the indexed columns.
     *
     * @return the columns
     */
    Column[] getColumns();

    /**
     * Get the index type.
     *
     * @return the index type
     */
    IndexType getIndexType();

    /**
     * Get the table on which this index is based.
     *
     * @return the table
     */
    Table getTable();

}
