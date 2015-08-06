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
package com.wplatform.ddal.command.expression;

import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.util.ValueHashMap;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueLong;
import com.wplatform.ddal.value.ValueNull;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataCount extends AggregateData {
    private long count;
    private ValueHashMap<AggregateDataCount> distinctValues;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        count++;
        if (distinct) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            distinctValues.put(v, this);
            return;
        }
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            if (distinctValues != null) {
                count = distinctValues.size();
            } else {
                count = 0;
            }
        }
        Value v = ValueLong.get(count);
        return v.convertTo(dataType);
    }

}
