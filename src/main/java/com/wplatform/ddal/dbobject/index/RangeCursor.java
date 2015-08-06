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

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueLong;

/**
 * The cursor implementation for the range index.
 */
class RangeCursor implements Cursor {

    private final long min, max;
    private boolean beforeFirst;
    private long current;
    private Row currentRow;

    RangeCursor(long min, long max) {
        this.min = min;
        this.max = max;
        beforeFirst = true;
    }

    @Override
    public Row get() {
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            current = min;
        } else {
            current++;
        }
        currentRow = new Row(new Value[]{ValueLong.get(current)}, 1);
        return current <= max;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
