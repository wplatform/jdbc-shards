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

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.TableFilter;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.value.*;

/**
 * Pattern matching comparison expression: WHERE NAME LIKE ?
 */
public class CompareLike extends Condition {

    private static final int MATCH = 0, ONE = 1, ANY = 2;

    private final CompareMode compareMode;
    private final String defaultEscape;
    private final boolean regexp;
    private Expression left;
    private Expression right;
    private Expression escape;
    private boolean isInit;
    private char[] patternChars;
    private String patternString;
    private int[] patternTypes;
    private int patternLength;
    private Pattern patternRegexp;

    private boolean ignoreCase;
    private boolean fastCompare;
    private boolean invalidPattern;

    public CompareLike(Database db, Expression left, Expression right,
                       Expression escape, boolean regexp) {
        this(db.getCompareMode(), db.getSettings().defaultEscape, left, right,
                escape, regexp);
    }

    public CompareLike(CompareMode compareMode, String defaultEscape,
                       Expression left, Expression right, Expression escape, boolean regexp) {
        this.compareMode = compareMode;
        this.defaultEscape = defaultEscape;
        this.regexp = regexp;
        this.left = left;
        this.right = right;
        this.escape = escape;
    }

    private static Character getEscapeChar(String s) {
        return s == null || s.length() == 0 ? null : s.charAt(0);
    }

    @Override
    public String getSQL() {
        String sql;
        if (regexp) {
            sql = left.getSQL() + " REGEXP " + right.getSQL();
        } else {
            sql = left.getSQL() + " LIKE " + right.getSQL();
            if (escape != null) {
                sql += " ESCAPE " + escape.getSQL();
            }
        }
        return "(" + sql + ")";
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        right = right.optimize(session);
        if (left.getType() == Value.STRING_IGNORECASE) {
            ignoreCase = true;
        }
        if (left.isValueSet()) {
            Value l = left.getValue(session);
            if (l == ValueNull.INSTANCE) {
                // NULL LIKE something > NULL
                return ValueExpression.getNull();
            }
        }
        if (escape != null) {
            escape = escape.optimize(session);
        }
        if (right.isValueSet() && (escape == null || escape.isValueSet())) {
            if (left.isValueSet()) {
                return ValueExpression.get(getValue(session));
            }
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                // something LIKE NULL > NULL
                return ValueExpression.getNull();
            }
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueExpression.getNull();
            }
            String p = r.getString();
            initPattern(p, getEscapeChar(e));
            if (invalidPattern) {
                return ValueExpression.getNull();
            }
            if ("%".equals(p)) {
                // optimization for X LIKE '%': convert to X IS NOT NULL
                return new Comparison(session,
                        Comparison.IS_NOT_NULL, left, null).optimize(session);
            }
            if (isFullMatch()) {
                // optimization for X LIKE 'Hello': convert to X = 'Hello'
                Value value = ValueString.get(patternString);
                Expression expr = ValueExpression.get(value);
                return new Comparison(session,
                        Comparison.EQUAL, left, expr).optimize(session);
            }
            isInit = true;
        }
        return this;
    }

    private Character getEscapeChar(Value e) {
        if (e == null) {
            return getEscapeChar(defaultEscape);
        }
        String es = e.getString();
        Character esc;
        if (es == null) {
            esc = getEscapeChar(defaultEscape);
        } else if (es.length() == 0) {
            esc = null;
        } else if (es.length() > 1) {
            throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, es);
        } else {
            esc = es.charAt(0);
        }
        return esc;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (regexp) {
            return;
        }
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        // parameters are always evaluatable, but
        // we need to check if the value is set
        // (at prepare time)
        // otherwise we would need to prepare at execute time,
        // which may be slower (possibly not in this case)
        if (!right.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return;
        }
        if (escape != null &&
                !escape.isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
            return;
        }
        String p = right.getValue(session).getString();
        Value e = escape == null ? null : escape.getValue(session);
        if (e == ValueNull.INSTANCE) {
            // should already be optimized
            DbException.throwInternalError();
        }
        initPattern(p, getEscapeChar(e));
        if (invalidPattern) {
            return;
        }
        if (patternLength <= 0 || patternTypes[0] != MATCH) {
            // can't use an index
            return;
        }
        int dataType = l.getColumn().getType();
        if (dataType != Value.STRING && dataType != Value.STRING_IGNORECASE &&
                dataType != Value.STRING_FIXED) {
            // column is not a varchar - can't use the index
            return;
        }
        int maxMatch = 0;
        StringBuilder buff = new StringBuilder();
        while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
            buff.append(patternChars[maxMatch++]);
        }
        String begin = buff.toString();
        if (maxMatch == patternLength) {
            filter.addIndexCondition(IndexCondition.get(Comparison.EQUAL, l,
                    ValueExpression.get(ValueString.get(begin))));
        } else {
            // TODO check if this is correct according to Unicode rules
            // (code points)
            String end;
            if (begin.length() > 0) {
                filter.addIndexCondition(IndexCondition.get(
                        Comparison.BIGGER_EQUAL, l,
                        ValueExpression.get(ValueString.get(begin))));
                char next = begin.charAt(begin.length() - 1);
                // search the 'next' unicode character (or at least a character
                // that is higher)
                for (int i = 1; i < 2000; i++) {
                    end = begin.substring(0, begin.length() - 1) + (char) (next + i);
                    if (compareMode.compareString(begin, end, ignoreCase) == -1) {
                        filter.addIndexCondition(IndexCondition.get(
                                Comparison.SMALLER, l,
                                ValueExpression.get(ValueString.get(end))));
                        break;
                    }
                }
            }
        }
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        if (!isInit) {
            Value r = right.getValue(session);
            if (r == ValueNull.INSTANCE) {
                return r;
            }
            String p = r.getString();
            Value e = escape == null ? null : escape.getValue(session);
            if (e == ValueNull.INSTANCE) {
                return ValueNull.INSTANCE;
            }
            initPattern(p, getEscapeChar(e));
        }
        if (invalidPattern) {
            return ValueNull.INSTANCE;
        }
        String value = l.getString();
        boolean result;
        if (regexp) {
            // result = patternRegexp.matcher(value).matches();
            result = patternRegexp.matcher(value).find();
        } else {
            result = compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
        }
        return ValueBoolean.get(result);
    }

    private boolean compare(char[] pattern, String s, int pi, int si) {
        return pattern[pi] == s.charAt(si) ||
                (!fastCompare && compareMode.equalsChars(patternString, pi, s,
                        si, ignoreCase));
    }

    private boolean compareAt(String s, int pi, int si, int sLen,
                              char[] pattern, int[] types) {
        for (; pi < patternLength; pi++) {
            switch (types[pi]) {
                case MATCH:
                    if ((si >= sLen) || !compare(pattern, s, pi, si++)) {
                        return false;
                    }
                    break;
                case ONE:
                    if (si++ >= sLen) {
                        return false;
                    }
                    break;
                case ANY:
                    if (++pi >= patternLength) {
                        return true;
                    }
                    while (si < sLen) {
                        if (compare(pattern, s, pi, si) &&
                                compareAt(s, pi, si, sLen, pattern, types)) {
                            return true;
                        }
                        si++;
                    }
                    return false;
                default:
                    DbException.throwInternalError();
            }
        }
        return si == sLen;
    }

    /**
     * Test if the value matches the pattern.
     *
     * @param testPattern the pattern
     * @param value       the value
     * @param escapeChar  the escape character
     * @return true if the value matches
     */
    public boolean test(String testPattern, String value, char escapeChar) {
        initPattern(testPattern, escapeChar);
        if (invalidPattern) {
            return false;
        }
        return compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
    }

    private void initPattern(String p, Character escapeChar) {
        if (compareMode.getName().equals(CompareMode.OFF) && !ignoreCase) {
            fastCompare = true;
        }
        if (regexp) {
            patternString = p;
            try {
                if (ignoreCase) {
                    patternRegexp = Pattern.compile(p, Pattern.CASE_INSENSITIVE);
                } else {
                    patternRegexp = Pattern.compile(p);
                }
            } catch (PatternSyntaxException e) {
                throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, p);
            }
            return;
        }
        patternLength = 0;
        if (p == null) {
            patternTypes = null;
            patternChars = null;
            return;
        }
        int len = p.length();
        patternChars = new char[len];
        patternTypes = new int[len];
        boolean lastAny = false;
        for (int i = 0; i < len; i++) {
            char c = p.charAt(i);
            int type;
            if (escapeChar != null && escapeChar == c) {
                if (i >= len - 1) {
                    invalidPattern = true;
                    return;
                }
                c = p.charAt(++i);
                type = MATCH;
                lastAny = false;
            } else if (c == '%') {
                if (lastAny) {
                    continue;
                }
                type = ANY;
                lastAny = true;
            } else if (c == '_') {
                type = ONE;
            } else {
                type = MATCH;
                lastAny = false;
            }
            patternTypes[patternLength] = type;
            patternChars[patternLength++] = c;
        }
        for (int i = 0; i < patternLength - 1; i++) {
            if ((patternTypes[i] == ANY) && (patternTypes[i + 1] == ONE)) {
                patternTypes[i] = ONE;
                patternTypes[i + 1] = ANY;
            }
        }
        patternString = new String(patternChars, 0, patternLength);
    }

    private boolean isFullMatch() {
        if (patternTypes == null) {
            return false;
        }
        for (int type : patternTypes) {
            if (type != MATCH) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
        if (escape != null) {
            escape.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
        if (escape != null) {
            escape.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void updateAggregate(Session session) {
        left.updateAggregate(session);
        right.updateAggregate(session);
        if (escape != null) {
            escape.updateAggregate(session);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor)
                && (escape == null || escape.isEverything(visitor));
    }

    @Override
    public int getCost() {
        return left.getCost() + right.getCost() + 3;
    }

    /* (non-Javadoc)
     * @see com.suning.snfddal.command.expression.Expression#exportParameters(java.util.List)
     */
    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        String sql;
        if (regexp) {
            sql = left.exportParameters(filter, container) + " REGEXP " + right.exportParameters(filter, container);
        } else {
            sql = left.exportParameters(filter, container) + " LIKE " + right.exportParameters(filter, container);
            if (escape != null) {
                sql += " ESCAPE " + escape.exportParameters(filter, container);
            }
        }
        return "(" + sql + ")";
    }

}
