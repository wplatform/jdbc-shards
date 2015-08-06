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
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.ValueHashMap;
import com.wplatform.ddal.value.*;

/**
 * Data stored while calculating an aggregate.
 */
class AggregateDataDefault extends AggregateData {
    private final int aggregateType;
    private long count;
    private ValueHashMap<AggregateDataDefault> distinctValues;
    private Value value;
    private double m2, mean;

    /**
     * @param aggregateType the type of the aggregate operation
     */
    AggregateDataDefault(int aggregateType) {
        this.aggregateType = aggregateType;
    }

    private static Value divide(Value a, long by) {
        if (by == 0) {
            return ValueNull.INSTANCE;
        }
        int type = Value.getHigherOrder(a.getType(), Value.LONG);
        Value b = ValueLong.get(by).convertTo(type);
        a = a.convertTo(type).divide(b);
        return a;
    }

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
        switch (aggregateType) {
            case Aggregate.SUM:
                if (value == null) {
                    value = v.convertTo(dataType);
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.AVG:
                if (value == null) {
                    value = v.convertTo(DataType.getAddProofType(dataType));
                } else {
                    v = v.convertTo(value.getType());
                    value = value.add(v);
                }
                break;
            case Aggregate.MIN:
                if (value == null || database.compare(v, value) < 0) {
                    value = v;
                }
                break;
            case Aggregate.MAX:
                if (value == null || database.compare(v, value) > 0) {
                    value = v;
                }
                break;
            case Aggregate.STDDEV_POP:
            case Aggregate.STDDEV_SAMP:
            case Aggregate.VAR_POP:
            case Aggregate.VAR_SAMP: {
                // Using Welford's method, see also
                // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
                // http://www.johndcook.com/standard_deviation.html
                double x = v.getDouble();
                if (count == 1) {
                    mean = x;
                    m2 = 0;
                } else {
                    double delta = x - mean;
                    mean += delta / count;
                    m2 += delta * (x - mean);
                }
                break;
            }
            case Aggregate.BOOL_AND:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean().booleanValue() &&
                            v.getBoolean().booleanValue());
                }
                break;
            case Aggregate.BOOL_OR:
                v = v.convertTo(Value.BOOLEAN);
                if (value == null) {
                    value = v;
                } else {
                    value = ValueBoolean.get(value.getBoolean().booleanValue() ||
                            v.getBoolean().booleanValue());
                }
                break;
            default:
                DbException.throwInternalError("type=" + aggregateType);
        }
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            count = 0;
            groupDistinct(database, dataType);
        }
        Value v = null;
        switch (aggregateType) {
            case Aggregate.SUM:
            case Aggregate.MIN:
            case Aggregate.MAX:
            case Aggregate.BOOL_OR:
            case Aggregate.BOOL_AND:
                v = value;
                break;
            case Aggregate.AVG:
                if (value != null) {
                    v = divide(value, count);
                }
                break;
            case Aggregate.STDDEV_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / count));
                break;
            }
            case Aggregate.STDDEV_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(Math.sqrt(m2 / (count - 1)));
                break;
            }
            case Aggregate.VAR_POP: {
                if (count < 1) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / count);
                break;
            }
            case Aggregate.VAR_SAMP: {
                if (count < 2) {
                    return ValueNull.INSTANCE;
                }
                v = ValueDouble.get(m2 / (count - 1));
                break;
            }
            default:
                DbException.throwInternalError("type=" + aggregateType);
        }
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

    private void groupDistinct(Database database, int dataType) {
        if (distinctValues == null) {
            return;
        }
        count = 0;
        for (Value v : distinctValues.keys()) {
            add(database, dataType, false, v);
        }
    }

}
