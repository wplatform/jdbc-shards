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

import java.text.Collator;
import java.util.Locale;

import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.util.StringUtils;

/**
 * Instances of this class can compare strings. Case sensitive and case
 * insensitive comparison is supported, and comparison using a collator.
 */
public class CompareMode {

    /**
     * This constant means there is no collator set, and the default string
     * comparison is to be used.
     */
    public static final String OFF = "OFF";

    /**
     * This constant means the default collator should be used, even if ICU4J is
     * in the classpath.
     */
    public static final String DEFAULT = "DEFAULT_";

    /**
     * This constant means ICU4J should be used (this will fail if it is not in
     * the classpath).
     */
    public static final String ICU4J = "ICU4J_";

    /**
     * This constant means that the BINARY columns are sorted as if the bytes
     * were signed.
     */
    public static final String SIGNED = "SIGNED";

    /**
     * This constant means that the BINARY columns are sorted as if the bytes
     * were unsigned.
     */
    public static final String UNSIGNED = "UNSIGNED";
    private static final boolean CAN_USE_ICU4J;
    private static CompareMode lastUsed;

    static {
        boolean b = false;
        try {
            Class.forName("com.ibm.icu.text.Collator");
            b = true;
        } catch (Exception e) {
            // ignore
        }
        CAN_USE_ICU4J = b;
    }

    private final String name;
    private final int strength;

    /**
     * If true, sort BINARY columns as if they contain unsigned bytes.
     */
    private final boolean binaryUnsigned;

    protected CompareMode(String name, int strength, boolean binaryUnsigned) {
        this.name = name;
        this.strength = strength;
        this.binaryUnsigned = binaryUnsigned;
    }

    /**
     * Create a new compare mode with the given collator and strength. If
     * required, a new CompareMode is created, or if possible the last one is
     * returned. A cache is used to speed up comparison when using a collator;
     * CollationKey objects are cached.
     *
     * @param name     the collation name or null
     * @param strength the collation strength
     * @return the compare mode
     */
    public static synchronized CompareMode getInstance(String name,
                                                       int strength) {
        return getInstance(name, strength, SysProperties.SORT_BINARY_UNSIGNED);
    }

    /**
     * Create a new compare mode with the given collator and strength. If
     * required, a new CompareMode is created, or if possible the last one is
     * returned. A cache is used to speed up comparison when using a collator;
     * CollationKey objects are cached.
     *
     * @param name           the collation name or null
     * @param strength       the collation strength
     * @param binaryUnsigned whether to compare binaries as unsigned
     * @return the compare mode
     */
    public static synchronized CompareMode getInstance(String name,
                                                       int strength, boolean binaryUnsigned) {
        if (lastUsed != null) {
            if (StringUtils.equals(lastUsed.name, name) &&
                    lastUsed.strength == strength &&
                    lastUsed.binaryUnsigned == binaryUnsigned) {
                return lastUsed;
            }
        }
        if (name == null || name.equals(OFF)) {
            lastUsed = new CompareMode(name, strength, binaryUnsigned);
        } else {
            boolean useICU4J;
            if (name.startsWith(ICU4J)) {
                useICU4J = true;
                name = name.substring(ICU4J.length());
            } else if (name.startsWith(DEFAULT)) {
                useICU4J = false;
                name = name.substring(DEFAULT.length());
            } else {
                useICU4J = CAN_USE_ICU4J;
            }
            if (useICU4J) {
                lastUsed = new CompareModeIcu4J(name, strength, binaryUnsigned);
            } else {
                lastUsed = new CompareModeDefault(name, strength, binaryUnsigned);
            }
        }
        return lastUsed;
    }

    /**
     * Get the collation name.
     *
     * @param l the locale
     * @return the name of the collation
     */
    public static String getName(Locale l) {
        Locale english = Locale.ENGLISH;
        String name = l.getDisplayLanguage(english) + ' ' +
                l.getDisplayCountry(english) + ' ' + l.getVariant();
        name = StringUtils.toUpperEnglish(name.trim().replace(' ', '_'));
        return name;
    }

    /**
     * Compare name name of the locale with the given name. The case of the name
     * is ignored.
     *
     * @param locale the locale
     * @param name   the name
     * @return true if they match
     */
    static boolean compareLocaleNames(Locale locale, String name) {
        return name.equalsIgnoreCase(locale.toString()) ||
                name.equalsIgnoreCase(getName(locale));
    }

    /**
     * Get the collator object for the given language name or language / country
     * combination.
     *
     * @param name the language name
     * @return the collator
     */
    public static Collator getCollator(String name) {
        Collator result = null;
        if (name.startsWith(ICU4J)) {
            name = name.substring(ICU4J.length());
        } else if (name.startsWith(DEFAULT)) {
            name = name.substring(DEFAULT.length());
        }
        if (name.length() == 2) {
            Locale locale = new Locale(StringUtils.toLowerEnglish(name), "");
            if (compareLocaleNames(locale, name)) {
                result = Collator.getInstance(locale);
            }
        } else if (name.length() == 5) {
            // LL_CC (language_country)
            int idx = name.indexOf('_');
            if (idx >= 0) {
                String language = StringUtils.toLowerEnglish(name.substring(0, idx));
                String country = name.substring(idx + 1);
                Locale locale = new Locale(language, country);
                if (compareLocaleNames(locale, name)) {
                    result = Collator.getInstance(locale);
                }
            }
        }
        if (result == null) {
            for (Locale locale : Collator.getAvailableLocales()) {
                if (compareLocaleNames(locale, name)) {
                    result = Collator.getInstance(locale);
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Compare two characters in a string.
     *
     * @param a          the first string
     * @param ai         the character index in the first string
     * @param b          the second string
     * @param bi         the character index in the second string
     * @param ignoreCase true if a case-insensitive comparison should be made
     * @return true if the characters are equals
     */
    public boolean equalsChars(String a, int ai, String b, int bi,
                               boolean ignoreCase) {
        char ca = a.charAt(ai);
        char cb = b.charAt(bi);
        if (ignoreCase) {
            ca = Character.toUpperCase(ca);
            cb = Character.toUpperCase(cb);
        }
        return ca == cb;
    }

    /**
     * Compare two strings.
     *
     * @param a          the first string
     * @param b          the second string
     * @param ignoreCase true if a case-insensitive comparison should be made
     * @return -1 if the first string is 'smaller', 1 if the second string is
     * smaller, and 0 if they are equal
     */
    public int compareString(String a, String b, boolean ignoreCase) {
        if (ignoreCase) {
            return a.compareToIgnoreCase(b);
        }
        return a.compareTo(b);
    }

    public String getName() {
        return name == null ? OFF : name;
    }

    public int getStrength() {
        return strength;
    }

    public boolean isBinaryUnsigned() {
        return binaryUnsigned;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof CompareMode)) {
            return false;
        }
        CompareMode o = (CompareMode) obj;
        if (!getName().equals(o.getName())) {
            return false;
        }
        if (strength != o.strength) {
            return false;
        }
        return binaryUnsigned == o.binaryUnsigned;
    }

    @Override
    public int hashCode() {
        return getName().hashCode() ^ strength ^ (binaryUnsigned ? -1 : 0);
    }

}
