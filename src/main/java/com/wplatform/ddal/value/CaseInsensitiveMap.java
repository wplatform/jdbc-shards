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

import java.util.HashMap;

import com.wplatform.ddal.util.StringUtils;

/**
 * A hash map with a case-insensitive string key.
 *
 * @param <V> the value type
 */
public class CaseInsensitiveMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = 1L;

    private static String toUpper(Object key) {
        return key == null ? null : StringUtils.toUpperEnglish(key.toString());
    }

    @Override
    public V get(Object key) {
        return super.get(toUpper(key));
    }

    @Override
    public V put(String key, V value) {
        return super.put(toUpper(key), value);
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(toUpper(key));
    }

    @Override
    public V remove(Object key) {
        return super.remove(toUpper(key));
    }

}
