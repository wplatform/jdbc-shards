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
package com.wplatform.ddal.engine;

import java.util.HashMap;

import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;

/**
 * The compatibility modes. There is a fixed set of modes (for example
 * PostgreSQL, MySQL). Each mode has different settings.
 */
public class Mode {

    /**
     * The name of the PostgreSQL mode.
     */
    public static final String POSTGRE_SQL = "PostgreSQL";

    /**
     * The name of the Oracle mode.
     */
    public static final String ORACLE = "Oracle";

    /**
     * The name of the MySQL mode.
     */
    public static final String MY_SQL = "MySQL";

    /**
     * The name of the MSSQLServer mode.
     */
    public static final String MSSQL_SERVER = "MSSQLServer";

    /**
     * The name of the HSQLDB mode.
     */
    public static final String HSQLDB = "HSQLDB";

    /**
     * The name of the Derby mode.
     */
    public static final String DERBY = "Derby";

    /**
     * The name of the DB2 mode.
     */
    public static final String DB2 = "DB2";

    /**
     * The name of the REGULAR mode.
     */
    public static final String REGULAR = "REGULAR";


    private static final HashMap<String, Mode> MODES = New.hashMap();

    // Modes are also documented in the features section

    static {
        Mode mode = new Mode(REGULAR);
        mode.nullConcatIsNull = true;
        add(mode);

        mode = new Mode(DB2);
        mode.aliasColumnName = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        add(mode);

        mode = new Mode(DERBY);
        mode.aliasColumnName = true;
        mode.uniqueIndexSingleNull = true;
        mode.supportOffsetFetch = true;
        mode.sysDummy1 = true;
        mode.isolationLevelInSelectOrInsertStatement = true;
        add(mode);

        mode = new Mode(HSQLDB);
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.nullConcatIsNull = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        add(mode);

        mode = new Mode(MSSQL_SERVER);
        mode.aliasColumnName = true;
        mode.squareBracketQuotedNames = true;
        mode.uniqueIndexSingleNull = true;
        mode.allowPlusForStringConcat = true;
        mode.swapConvertFunctionParameters = true;
        add(mode);

        mode = new Mode(MY_SQL);
        mode.convertInsertNullToZero = true;
        mode.indexDefinitionInCreateTable = true;
        mode.lowerCaseIdentifiers = true;
        mode.onDuplicateKeyUpdate = true;
        add(mode);

        mode = new Mode(ORACLE);
        mode.aliasColumnName = true;
        mode.convertOnlyToSmallerScale = true;
        mode.uniqueIndexSingleNullExceptAllColumnsAreNull = true;
        mode.treatEmptyStringsAsNull = true;
        add(mode);

        mode = new Mode(POSTGRE_SQL);
        mode.aliasColumnName = true;
        mode.nullConcatIsNull = true;
        mode.supportOffsetFetch = true;
        mode.systemColumns = true;
        mode.logIsLogBase10 = true;
        mode.serialColumnIsNotPK = true;
        add(mode);
    }

    private final String name;
    /**
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     */
    public boolean aliasColumnName;
    /**
     * When inserting data, if a column is defined to be NOT NULL and NULL is
     * inserted, then a 0 (or empty string, or the current timestamp for
     * timestamp columns) value is used. Usually, this operation is not allowed
     * and an exception is thrown.
     */
    public boolean convertInsertNullToZero;
    /**
     * When converting the scale of decimal data, the number is only converted
     * if the new scale is smaller than the current scale. Usually, the scale is
     * converted and 0s are added if required.
     */
    public boolean convertOnlyToSmallerScale;
    /**
     * Creating indexes in the CREATE TABLE statement is allowed using
     * <code>INDEX(..)</code> or <code>KEY(..)</code>.
     * Example: <code>create table test(id int primary key, name varchar(255),
     * key idx_name(name));</code>
     */
    public boolean indexDefinitionInCreateTable;
    /**
     * Meta data calls return identifiers in lower case.
     */
    public boolean lowerCaseIdentifiers;
    /**
     * Concatenation with NULL results in NULL. Usually, NULL is treated as an
     * empty string if only one of the operands is NULL, and NULL is only
     * returned if both operands are NULL.
     */
    public boolean nullConcatIsNull;
    /**
     * Identifiers may be quoted using square brackets as in [Test].
     */
    public boolean squareBracketQuotedNames;
    /**
     * Support for the syntax
     * [OFFSET .. ROW|ROWS] [FETCH FIRST .. ROW|ROWS ONLY]
     * as an alternative for LIMIT .. OFFSET.
     */
    public boolean supportOffsetFetch = true;
    /**
     * The system columns 'CTID' and 'OID' are supported.
     */
    public boolean systemColumns;
    /**
     * For unique indexes, NULL is distinct. That means only one row with NULL
     * in one of the columns is allowed.
     */
    public boolean uniqueIndexSingleNull;
    /**
     * When using unique indexes, multiple rows with NULL in all columns
     * are allowed, however it is not allowed to have multiple rows with the
     * same values otherwise.
     */
    public boolean uniqueIndexSingleNullExceptAllColumnsAreNull;
    /**
     * Empty strings are treated like NULL values. Useful for Oracle emulation.
     */
    public boolean treatEmptyStringsAsNull;
    /**
     * Support the pseudo-table SYSIBM.SYSDUMMY1.
     */
    public boolean sysDummy1;
    /**
     * Text can be concatenated using '+'.
     */
    public boolean allowPlusForStringConcat;
    /**
     * The function LOG() uses base 10 instead of E.
     */
    public boolean logIsLogBase10;
    /**
     * SERIAL and BIGSERIAL columns are not automatically primary keys.
     */
    public boolean serialColumnIsNotPK;
    /**
     * Swap the parameters of the CONVERT function.
     */
    public boolean swapConvertFunctionParameters;
    /**
     * can set the isolation level using WITH {RR|RS|CS|UR}
     */
    public boolean isolationLevelInSelectOrInsertStatement;
    /**
     * MySQL style INSERT ... ON DUPLICATE KEY UPDATE ...
     */
    public boolean onDuplicateKeyUpdate;

    private Mode(String name) {
        this.name = name;
    }

    private static void add(Mode mode) {
        MODES.put(StringUtils.toUpperEnglish(mode.name), mode);
    }

    /**
     * Get the mode with the given name.
     *
     * @param name the name of the mode
     * @return the mode object
     */
    public static Mode getInstance(String name) {
        return MODES.get(StringUtils.toUpperEnglish(name));
    }

    public String getName() {
        return name;
    }

}
