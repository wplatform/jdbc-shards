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
package com.wplatform.ddal.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.message.TraceObject;
import com.wplatform.ddal.util.StringUtils;

/**
 * A savepoint is a point inside a transaction to where a transaction can be
 * rolled back. The tasks that where done before the savepoint are not rolled
 * back in this case.
 */
public class JdbcSavepoint extends TraceObject implements Savepoint {

    private static final String SYSTEM_SAVEPOINT_PREFIX = "SYSTEM_SAVEPOINT_";

    private final int savepointId;
    private final String name;
    private JdbcConnection conn;

    JdbcSavepoint(JdbcConnection conn, int savepointId, String name,
                  Trace trace, int id) {
        setTrace(trace, TraceObject.SAVEPOINT, id);
        this.conn = conn;
        this.savepointId = savepointId;
        this.name = name;
    }

    /**
     * Get the savepoint name for this name or id.
     * If the name is null, the id is used.
     *
     * @param name the name (may be null)
     * @param id   the id
     * @return the savepoint name
     */
    static String getName(String name, int id) {
        if (name != null) {
            return StringUtils.quoteJavaString(name);
        }
        return SYSTEM_SAVEPOINT_PREFIX + id;
    }

    /**
     * Release this savepoint. This method only set the connection to null and
     * does not execute a statement.
     */
    void release() {
        this.conn = null;
    }

    /**
     * Roll back to this savepoint.
     */
    void rollback() {
        checkValid();
        conn.prepareCommand(
                "ROLLBACK TO SAVEPOINT " + getName(name, savepointId),
                Integer.MAX_VALUE).executeUpdate();
    }

    private void checkValid() {
        if (conn == null) {
            throw DbException.get(ErrorCode.SAVEPOINT_IS_INVALID_1,
                    getName(name, savepointId));
        }
    }

    /**
     * Get the generated id of this savepoint.
     *
     * @return the id
     */
    @Override
    public int getSavepointId() throws SQLException {
        try {
            debugCodeCall("getSavepointId");
            checkValid();
            if (name != null) {
                throw DbException.get(ErrorCode.SAVEPOINT_IS_NAMED);
            }
            return savepointId;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the name of this savepoint.
     *
     * @return the name
     */
    @Override
    public String getSavepointName() throws SQLException {
        try {
            debugCodeCall("getSavepointName");
            checkValid();
            if (name == null) {
                throw DbException.get(ErrorCode.SAVEPOINT_IS_UNNAMED);
            }
            return name;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": id=" + savepointId + " name=" + name;
    }


}
