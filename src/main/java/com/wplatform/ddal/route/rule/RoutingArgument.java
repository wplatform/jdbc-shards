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
package com.wplatform.ddal.route.rule;

import java.util.List;

import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class RoutingArgument {
    
    public static final int NONE_ROUTING_ARGUMENT = 0;
    public static final int FIXED_ROUTING_ARGUMENT = 1;
    public static final int RANGE_ROUTING_ARGUMENT = 2;

    private int argumentType;

    private List<Value> values;
    
    private Value start;
    
    private Value end;

    public RoutingArgument() {
        this.argumentType = NONE_ROUTING_ARGUMENT;
    }

    public RoutingArgument(List<Value> values) {
        this.argumentType = FIXED_ROUTING_ARGUMENT;
        this.values = values;
    }

    public RoutingArgument(Value start, Value end) {
        this.argumentType = RANGE_ROUTING_ARGUMENT;
        this.start = start;
        this.end = end;
    }

    public int getArgumentType() {
        return argumentType;
    }

    public List<Value> getValues() {
        return values;
    }

    public Value getStart() {
        return start;
    }

    public Value getEnd() {
        return end;
    }
}
