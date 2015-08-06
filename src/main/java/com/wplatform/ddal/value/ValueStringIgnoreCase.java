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
package com.wplatform.ddal.value;

import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.util.StringUtils;

/**
 * Implementation of the VARCHAR_IGNORECASE data type.
 */
public class ValueStringIgnoreCase extends ValueString {

    private static final ValueStringIgnoreCase EMPTY =
            new ValueStringIgnoreCase("");
    private int hash;

    protected ValueStringIgnoreCase(String value) {
        super(value);
    }

    /**
     * Get or create a case insensitive string value for the given string.
     * The value will have the same case as the passed string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueStringIgnoreCase get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueStringIgnoreCase obj = new ValueStringIgnoreCase(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        ValueStringIgnoreCase cache = (ValueStringIgnoreCase) Value.cache(obj);
        // the cached object could have the wrong case
        // (it would still be 'equal', but we don't like to store it)
        if (cache.value.equals(s)) {
            return cache;
        }
        return obj;
    }

    @Override
    public int getType() {
        return Value.STRING_IGNORECASE;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueStringIgnoreCase v = (ValueStringIgnoreCase) o;
        return mode.compareString(value, v.value, true);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString
                && value.equalsIgnoreCase(((ValueString) other).value);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            // this is locale sensitive
            hash = value.toUpperCase().hashCode();
        }
        return hash;
    }

    @Override
    public String getSQL() {
        return "CAST(" + StringUtils.quoteStringSQL(value) + " AS VARCHAR_IGNORECASE)";
    }

    @Override
    protected ValueString getNew(String s) {
        return ValueStringIgnoreCase.get(s);
    }

}
