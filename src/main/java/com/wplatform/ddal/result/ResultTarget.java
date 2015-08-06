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
 * A object where rows are written to.
 */
public interface ResultTarget {

    /**
     * Add the row to the result set.
     *
     * @param values the values
     */
    void addRow(Value[] values);

    /**
     * Get the number of rows.
     *
     * @return the number of rows
     */
    int getRowCount();

}
