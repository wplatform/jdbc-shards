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

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.Locale;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.JdbcUtils;
import com.wplatform.ddal.util.StringUtils;

/**
 * An implementation of CompareMode that uses the ICU4J Collator.
 */
public class CompareModeIcu4J extends CompareMode {

    private final Comparator<String> collator;

    protected CompareModeIcu4J(String name, int strength, boolean binaryUnsigned) {
        super(name, strength, binaryUnsigned);
        collator = getIcu4jCollator(name, strength);
    }

    @SuppressWarnings("unchecked")
    private static Comparator<String> getIcu4jCollator(String name, int strength) {
        try {
            Comparator<String> result = null;
            Class<?> collatorClass = JdbcUtils.loadUserClass(
                    "com.ibm.icu.text.Collator");
            Method getInstanceMethod = collatorClass.getMethod(
                    "getInstance", Locale.class);
            if (name.length() == 2) {
                Locale locale = new Locale(StringUtils.toLowerEnglish(name), "");
                if (compareLocaleNames(locale, name)) {
                    result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                }
            } else if (name.length() == 5) {
                // LL_CC (language_country)
                int idx = name.indexOf('_');
                if (idx >= 0) {
                    String language = StringUtils.toLowerEnglish(name.substring(0, idx));
                    String country = name.substring(idx + 1);
                    Locale locale = new Locale(language, country);
                    if (compareLocaleNames(locale, name)) {
                        result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                    }
                }
            }
            if (result == null) {
                for (Locale locale : (Locale[]) collatorClass.getMethod(
                        "getAvailableLocales").invoke(null)) {
                    if (compareLocaleNames(locale, name)) {
                        result = (Comparator<String>) getInstanceMethod.invoke(null, locale);
                        break;
                    }
                }
            }
            if (result == null) {
                throw DbException.getInvalidValueException("collator", name);
            }
            collatorClass.getMethod("setStrength", int.class).invoke(result, strength);
            return result;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public int compareString(String a, String b, boolean ignoreCase) {
        if (ignoreCase) {
            a = a.toUpperCase();
            b = b.toUpperCase();
        }
        return collator.compare(a, b);
    }

    @Override
    public boolean equalsChars(String a, int ai, String b, int bi,
                               boolean ignoreCase) {
        return compareString(a.substring(ai, ai + 1), b.substring(bi, bi + 1),
                ignoreCase) == 0;
    }

}
