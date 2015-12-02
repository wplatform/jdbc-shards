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
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.value.Value;

/**
 * A cursor for a function that returns a result.
 */
public class FunctionCursor implements Cursor {

    private final ResultInterface result;
    private Value[] values;
    private Row row;

    FunctionCursor(ResultInterface result) {
        this.result = result;
    }

    @Override
    public Row get() {
        if (values == null) {
            return null;
        }
        if (row == null) {
            row = new Row(values, 1);
        }
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        row = null;
        if (result != null && result.next()) {
            values = result.currentRow();
        } else {
            values = null;
        }
        return values != null;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
