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
package com.wplatform.ddal.command.dml;

import java.text.Collator;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ValueExpression;
import com.wplatform.ddal.dbobject.Setting;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Mode;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.value.CompareMode;
import com.wplatform.ddal.value.ValueInt;

/**
 * This class represents the statement
 * SET
 */
public class Set extends Prepared {

    private final int type;
    private Expression expression;
    private String stringValue;
    private String[] stringValueList;

    public Set(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setString(String v) {
        this.stringValue = v;
    }

    @Override
    public boolean isTransactional() {
        switch (type) {
            case SetTypes.CLUSTER:
            case SetTypes.VARIABLE:
            case SetTypes.QUERY_TIMEOUT:
            case SetTypes.LOCK_TIMEOUT:
            case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            case SetTypes.TRACE_LEVEL_FILE:
            case SetTypes.THROTTLE:
            case SetTypes.SCHEMA:
            case SetTypes.SCHEMA_SEARCH_PATH:
            case SetTypes.RETENTION_TIME:
                return true;
            default:
        }
        return false;
    }

    @Override
    public int update() {
        Database database = session.getDatabase();
        String name = SetTypes.getTypeName(type);
        switch (type) {
            case SetTypes.ALLOW_LITERALS: {
                session.getUser().checkAdmin();
                int value = getIntValue();
                if (value < 0 || value > 2) {
                    throw DbException.getInvalidValueException("ALLOW_LITERALS",
                            getIntValue());
                }
                database.setAllowLiterals(value);
                addOrUpdateSetting(name, null, value);
                break;
            }
            case SetTypes.COLLATION: {
                session.getUser().checkAdmin();
                final boolean binaryUnsigned = database.
                        getCompareMode().isBinaryUnsigned();
                CompareMode compareMode;
                StringBuilder buff = new StringBuilder(stringValue);
                if (stringValue.equals(CompareMode.OFF)) {
                    compareMode = CompareMode.getInstance(null, 0, binaryUnsigned);
                } else {
                    int strength = getIntValue();
                    buff.append(" STRENGTH ");
                    if (strength == Collator.IDENTICAL) {
                        buff.append("IDENTICAL");
                    } else if (strength == Collator.PRIMARY) {
                        buff.append("PRIMARY");
                    } else if (strength == Collator.SECONDARY) {
                        buff.append("SECONDARY");
                    } else if (strength == Collator.TERTIARY) {
                        buff.append("TERTIARY");
                    }
                    compareMode = CompareMode.getInstance(stringValue, strength,
                            binaryUnsigned);
                }
                CompareMode old = database.getCompareMode();
                if (old.equals(compareMode)) {
                    break;
                }
                addOrUpdateSetting(name, buff.toString(), 0);
                database.setCompareMode(compareMode);
                break;
            }
            case SetTypes.BINARY_COLLATION: {
                session.getUser().checkAdmin();
                CompareMode currentMode = database.getCompareMode();
                CompareMode newMode;
                if (stringValue.equals(CompareMode.SIGNED)) {
                    newMode = CompareMode.getInstance(currentMode.getName(),
                            currentMode.getStrength(), false);
                } else if (stringValue.equals(CompareMode.UNSIGNED)) {
                    newMode = CompareMode.getInstance(currentMode.getName(),
                            currentMode.getStrength(), true);
                } else {
                    throw DbException.getInvalidValueException("BINARY_COLLATION",
                            stringValue);
                }
                addOrUpdateSetting(name, stringValue, 0);
                database.setCompareMode(newMode);
                break;
            }
            case SetTypes.IGNORECASE:
                session.getUser().checkAdmin();
                database.setIgnoreCase(getIntValue() == 1);
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.LOCK_MODE:
                session.getUser().checkAdmin();
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.LOCK_TIMEOUT:
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException("LOCK_TIMEOUT",
                            getIntValue());
                }
                session.setLockTimeout(getIntValue());
                break;
            case SetTypes.MAX_MEMORY_ROWS: {
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException("MAX_MEMORY_ROWS",
                            getIntValue());
                }
                session.getUser().checkAdmin();
                database.setMaxMemoryRows(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.MAX_OPERATION_MEMORY: {
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException(
                            "MAX_OPERATION_MEMORY", getIntValue());
                }
                session.getUser().checkAdmin();
                int value = getIntValue();
                database.setMaxOperationMemory(value);
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
            case SetTypes.QUERY_TIMEOUT: {
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException("QUERY_TIMEOUT",
                            getIntValue());
                }
                int value = getIntValue();
                session.setQueryTimeout(value);
                break;
            }
            case SetTypes.SCHEMA: {
                Schema schema = database.getSchema(stringValue);
                session.setCurrentSchema(schema);
                break;
            }
            case SetTypes.SCHEMA_SEARCH_PATH: {
                session.setSchemaSearchPath(stringValueList);
                break;
            }
            case SetTypes.TRACE_LEVEL_FILE:
                session.getUser().checkAdmin();
                if (getCurrentObjectId() == 0) {
                    // don't set the property when opening the database
                    // this is for compatibility with older versions, because
                    // this setting was persistent
                    database.getTraceSystem().setLevelFile(getIntValue());
                }
                break;
            case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
                session.getUser().checkAdmin();
                if (getCurrentObjectId() == 0) {
                    // don't set the property when opening the database
                    // this is for compatibility with older versions, because
                    // this setting was persistent
                    database.getTraceSystem().setLevelSystemOut(getIntValue());
                }
                break;
            case SetTypes.TRACE_MAX_FILE_SIZE: {
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException(
                            "TRACE_MAX_FILE_SIZE", getIntValue());
                }
                session.getUser().checkAdmin();
                int size = getIntValue() * 1024 * 1024;
                database.getTraceSystem().setMaxFileSize(size);
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.THROTTLE: {
                if (getIntValue() < 0) {
                    throw DbException.getInvalidValueException("THROTTLE",
                            getIntValue());
                }
                session.setThrottle(getIntValue());
                break;
            }
            case SetTypes.VARIABLE: {
                Expression expr = expression.optimize(session);
                session.setVariable(stringValue, expr.getValue(session));
                break;
            }
            case SetTypes.COMPRESS_LOB:
            case SetTypes.CREATE_BUILD:
            case SetTypes.DATABASE_EVENT_LISTENER:
            case SetTypes.DB_CLOSE_DELAY:
            case SetTypes.DEFAULT_LOCK_TIMEOUT:
            case SetTypes.DEFAULT_TABLE_TYPE:
            case SetTypes.EXCLUSIVE:
            case SetTypes.JAVA_OBJECT_SERIALIZER:
            case SetTypes.LOG:
            case SetTypes.MAX_LENGTH_INPLACE_LOB:
            case SetTypes.MAX_LOG_SIZE:
            case SetTypes.MAX_MEMORY_UNDO:
            case SetTypes.MULTI_THREADED:
            case SetTypes.MVCC:
            case SetTypes.OPTIMIZE_REUSE_RESULTS:
            case SetTypes.REDO_LOG_BINARY:
            case SetTypes.REFERENTIAL_INTEGRITY:
            case SetTypes.QUERY_STATISTICS:
            case SetTypes.UNDO_LOG:
            case SetTypes.CACHE_SIZE:
            case SetTypes.CLUSTER:
            case SetTypes.WRITE_DELAY:
            case SetTypes.RETENTION_TIME:
                DbException.throwInternalError("type=" + type);
                break;
            default:
                DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    private int getIntValue() {
        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    public void setInt(int value) {
        this.expression = ValueExpression.get(ValueInt.get(value));
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    private void addOrUpdateSetting(String name, String s, int v) {
        addOrUpdateSetting(session, name, s, v);
    }

    private void addOrUpdateSetting(Session session, String name, String s,
                                    int v) {
        Database database = session.getDatabase();
        Setting setting = database.findSetting(name);
        boolean addNew = false;
        if (setting == null) {
            addNew = true;
            int id = getObjectId();
            setting = new Setting(database, id, name);
        }
        if (s != null) {
            if (!addNew && setting.getStringValue().equals(s)) {
                return;
            }
            setting.setStringValue(s);
        } else {
            if (!addNew && setting.getIntValue() == v) {
                return;
            }
            setting.setIntValue(v);
        }
        if (addNew) {
            database.addDatabaseObject(setting);
        }
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    public void setStringArray(String[] list) {
        this.stringValueList = list;
    }

    @Override
    public int getType() {
        return CommandInterface.SET;
    }

}
