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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.BatchUpdateException;
import java.sql.SQLException;

/**
 * Represents a batch update database exception.
 */
public class JdbcBatchUpdateException extends BatchUpdateException {

    private static final long serialVersionUID = 1L;

    /**
     * INTERNAL
     */
    JdbcBatchUpdateException(SQLException next, int[] updateCounts) {
        super(next.getMessage(), next.getSQLState(), next.getErrorCode(), updateCounts);
        setNextException(next);
    }

    /**
     * INTERNAL
     */
    @Override
    public void printStackTrace() {
        // The default implementation already does that,
        // but we do it again to avoid problems.
        // If it is not implemented, somebody might implement it
        // later on which would be a problem if done in the wrong way.
        printStackTrace(System.err);
    }

    /**
     * INTERNAL
     */
    @Override
    public void printStackTrace(PrintWriter s) {
        if (s != null) {
            super.printStackTrace(s);
            if (getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

    /**
     * INTERNAL
     */
    @Override
    public void printStackTrace(PrintStream s) {
        if (s != null) {
            super.printStackTrace(s);
            if (getNextException() != null) {
                getNextException().printStackTrace(s);
            }
        }
    }

}
