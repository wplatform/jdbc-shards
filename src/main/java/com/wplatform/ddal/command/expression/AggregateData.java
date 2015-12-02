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
import com.wplatform.ddal.value.Value;

/**
 * Abstract class for the computation of an aggregate.
 */
abstract class AggregateData {

    /**
     * Create an AggregateData object of the correct sub-type.
     *
     * @param aggregateType the type of the aggregate operation
     * @return the aggregate data object of the specified type
     */
    static AggregateData create(int aggregateType) {
        if (aggregateType == Aggregate.SELECTIVITY) {
            return new AggregateDataSelectivity();
        } else if (aggregateType == Aggregate.GROUP_CONCAT) {
            return new AggregateDataGroupConcat();
        } else if (aggregateType == Aggregate.COUNT_ALL) {
            return new AggregateDataCountAll();
        } else if (aggregateType == Aggregate.COUNT) {
            return new AggregateDataCount();
        } else if (aggregateType == Aggregate.HISTOGRAM) {
            return new AggregateDataHistogram();
        } else {
            return new AggregateDataDefault(aggregateType);
        }
    }

    /**
     * Add a value to this aggregate.
     *
     * @param database the database
     * @param dataType the datatype of the computed result
     * @param distinct if the calculation should be distinct
     * @param v        the value
     */
    abstract void add(Database database, int dataType, boolean distinct, Value v);

    /**
     * Get the aggregate result.
     *
     * @param database the database
     * @param dataType the datatype of the computed result
     * @param distinct if distinct is used
     * @return the value
     */
    abstract Value getValue(Database database, int dataType, boolean distinct);
}
