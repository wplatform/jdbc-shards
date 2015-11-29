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
package com.wplatform.ddal.excutor.dml;

import com.wplatform.ddal.command.dml.Set;
import com.wplatform.ddal.command.dml.SetTypes;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Mode;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class SetExecutor extends CommonPreparedExecutor<Set> {

    /**
     * @param prepared
     */
    public SetExecutor(Set prepared) {
        super(prepared);
    }

    
    @Override
    public int executeUpdate() {
        Database database = session.getDatabase();
        String stringValue = prepared.getStringValue();
        int type = prepared.getSetType();
        switch (type) {
        case SetTypes.QUERY_TIMEOUT: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("QUERY_TIMEOUT",
                        getIntValue());
            }
            int value = getIntValue();
            session.setQueryTimeout(value);
            break;
        }
        case SetTypes.ALLOW_LITERALS: {
            session.getUser().checkAdmin();
            int value = getIntValue();
            if (value < 0 || value > 2) {
                throw DbException.getInvalidValueException("ALLOW_LITERALS",
                        getIntValue());
            }
            database.setAllowLiterals(value);
            break;
        }
        case SetTypes.MAX_MEMORY_ROWS: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("MAX_MEMORY_ROWS",
                        getIntValue());
            }
            session.getUser().checkAdmin();
            database.setMaxMemoryRows(getIntValue());
            break;
        }
        case SetTypes.MODE:
            Mode mode = Mode.getInstance(stringValue);
            if (mode == null) {
                throw DbException.get(ErrorCode.UNKNOWN_MODE_1, stringValue);
            }
            if (database.getMode() != mode) {
                session.getUser().checkAdmin();
                database.setMode(mode);
            }
            break;
        case SetTypes.SCHEMA: {
            Schema schema = database.getSchema(stringValue);
            session.setCurrentSchema(schema);
            break;
        }
        case SetTypes.TRACE_LEVEL_FILE:
            session.getUser().checkAdmin();
            database.getTraceSystem().setLevelFile(getIntValue());
            break;
        case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            session.getUser().checkAdmin();
            database.getTraceSystem().setLevelSystemOut(getIntValue());
            break;
        case SetTypes.THROTTLE: {
            if (getIntValue() < 0) {
                throw DbException.getInvalidValueException("THROTTLE",
                        getIntValue());
            }
            session.setThrottle(getIntValue());
            break;
        }
        case SetTypes.CACHE_SIZE:
        case SetTypes.CLUSTER: 
        case SetTypes.COLLATION: 
        case SetTypes.BINARY_COLLATION:
        case SetTypes.COMPRESS_LOB: 
        case SetTypes.CREATE_BUILD: 
        case SetTypes.DATABASE_EVENT_LISTENER:
        case SetTypes.DB_CLOSE_DELAY:
        case SetTypes.DEFAULT_LOCK_TIMEOUT:
        case SetTypes.DEFAULT_TABLE_TYPE:
        case SetTypes.EXCLUSIVE: 
        case SetTypes.JAVA_OBJECT_SERIALIZER: 
        case SetTypes.IGNORECASE:
        case SetTypes.LOCK_MODE:
        case SetTypes.LOCK_TIMEOUT:
        case SetTypes.LOG:
        case SetTypes.MAX_LENGTH_INPLACE_LOB: 
        case SetTypes.MAX_LOG_SIZE:
        case SetTypes.MAX_MEMORY_UNDO: 
        case SetTypes.MAX_OPERATION_MEMORY: 
        case SetTypes.MULTI_THREADED: 
        case SetTypes.MVCC: 
        case SetTypes.OPTIMIZE_REUSE_RESULTS: 
        case SetTypes.REDO_LOG_BINARY: 
        case SetTypes.REFERENTIAL_INTEGRITY: 
        case SetTypes.QUERY_STATISTICS: 
        case SetTypes.SCHEMA_SEARCH_PATH: 
        case SetTypes.TRACE_MAX_FILE_SIZE: 
        case SetTypes.UNDO_LOG: 
        case SetTypes.VARIABLE: 
        case SetTypes.WRITE_DELAY: 
        case SetTypes.RETENTION_TIME:
        default:
            DbException.throwInternalError("type="+type);
        }
        return 0;
    }

    private int getIntValue() {
        Expression expression = prepared.getExpression();
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

 
}
