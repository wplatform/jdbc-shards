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

import java.util.List;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class MergedCursor implements Cursor {

    private List<ResultCursor> cursors;
    private ResultCursor currentCursor;
    private Row currentRow;
    private int index = 0;

    public MergedCursor(List<ResultCursor> cursors) {
        super();
        this.cursors = cursors;
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
        while (index < cursors.size()) {
            if (currentCursor == null) {
                currentCursor = cursors.get(index);
            }
            boolean result = currentCursor.next();
            if (!result) {
                currentCursor = null;
                ++index;
            } else {
                currentRow = currentCursor.get();
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

}
