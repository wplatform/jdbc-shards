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
package com.wplatform.ddal.dbobject.index;

import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;

/**
 * A cursor is a helper object to iterate through an index.
 * For indexes are sorted (such as the b tree index), it can iterate
 * to the very end of the index. For other indexes that don't support
 * that (such as a hash index), only one row is returned.
 * The cursor is initially positioned before the first row, that means
 * next() must be called before accessing data.
 */
public interface Cursor {

    /**
     * Get the complete current row.
     * All column are available.
     *
     * @return the complete row
     */
    Row get();

    /**
     * Get the current row.
     * Only the data for indexed columns is available in this row.
     *
     * @return the search row
     */
    SearchRow getSearchRow();

    /**
     * Skip to the next row if one is available.
     *
     * @return true if another row is available
     */
    boolean next();

    /**
     * Skip to the previous row if one is available.
     * No filtering is made here.
     *
     * @return true if another row is available
     */
    boolean previous();

}
