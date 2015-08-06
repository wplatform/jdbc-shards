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

import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueBoolean;

/**
 * Represents a condition returning a boolean value, or NULL.
 */
abstract class Condition extends Expression {

    @Override
    public int getType() {
        return Value.BOOLEAN;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueBoolean.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueBoolean.DISPLAY_SIZE;
    }

}
