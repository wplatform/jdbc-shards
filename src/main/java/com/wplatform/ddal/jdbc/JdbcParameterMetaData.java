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

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.expression.ParameterInterface;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.message.TraceObject;
import com.wplatform.ddal.util.MathUtils;
import com.wplatform.ddal.value.DataType;
import com.wplatform.ddal.value.Value;

/**
 * Information about the parameters of a prepared statement.
 */
public class JdbcParameterMetaData extends TraceObject implements
        ParameterMetaData {

    private final JdbcPreparedStatement prep;
    private final int paramCount;
    private final ArrayList<? extends ParameterInterface> parameters;

    JdbcParameterMetaData(Trace trace, JdbcPreparedStatement prep,
                          CommandInterface command, int id) {
        setTrace(trace, TraceObject.PARAMETER_META_DATA, id);
        this.prep = prep;
        this.parameters = command.getParameters();
        this.paramCount = parameters.size();
    }

    /**
     * Returns the number of parameters.
     *
     * @return the number
     */
    @Override
    public int getParameterCount() throws SQLException {
        try {
            debugCodeCall("getParameterCount");
            checkClosed();
            return paramCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter mode.
     * Always returns parameterModeIn.
     *
     * @param param the column index (1,2,...)
     * @return parameterModeIn
     */
    @Override
    public int getParameterMode(int param) throws SQLException {
        try {
            debugCodeCall("getParameterMode", param);
            getParameter(param);
            return parameterModeIn;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter type.
     * java.sql.Types.VARCHAR is returned if the data type is not known.
     *
     * @param param the column index (1,2,...)
     * @return the data type
     */
    @Override
    public int getParameterType(int param) throws SQLException {
        try {
            debugCodeCall("getParameterType", param);
            ParameterInterface p = getParameter(param);
            int type = p.getType();
            if (type == Value.UNKNOWN) {
                type = Value.STRING;
            }
            return DataType.getDataType(type).sqlType;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter precision.
     * The value 0 is returned if the precision is not known.
     *
     * @param param the column index (1,2,...)
     * @return the precision
     */
    @Override
    public int getPrecision(int param) throws SQLException {
        try {
            debugCodeCall("getPrecision", param);
            ParameterInterface p = getParameter(param);
            return MathUtils.convertLongToInt(p.getPrecision());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter scale.
     * The value 0 is returned if the scale is not known.
     *
     * @param param the column index (1,2,...)
     * @return the scale
     */
    @Override
    public int getScale(int param) throws SQLException {
        try {
            debugCodeCall("getScale", param);
            ParameterInterface p = getParameter(param);
            return p.getScale();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this is nullable parameter.
     * Returns ResultSetMetaData.columnNullableUnknown..
     *
     * @param param the column index (1,2,...)
     * @return ResultSetMetaData.columnNullableUnknown
     */
    @Override
    public int isNullable(int param) throws SQLException {
        try {
            debugCodeCall("isNullable", param);
            return getParameter(param).getNullable();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this parameter is signed.
     * It always returns true.
     *
     * @param param the column index (1,2,...)
     * @return true
     */
    @Override
    public boolean isSigned(int param) throws SQLException {
        try {
            debugCodeCall("isSigned", param);
            getParameter(param);
            return true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the Java class name of the parameter.
     * "java.lang.String" is returned if the type is not known.
     *
     * @param param the column index (1,2,...)
     * @return the Java class name
     */
    @Override
    public String getParameterClassName(int param) throws SQLException {
        try {
            debugCodeCall("getParameterClassName", param);
            ParameterInterface p = getParameter(param);
            int type = p.getType();
            if (type == Value.UNKNOWN) {
                type = Value.STRING;
            }
            return DataType.getTypeClassName(type);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the parameter type name.
     * "VARCHAR" is returned if the type is not known.
     *
     * @param param the column index (1,2,...)
     * @return the type name
     */
    @Override
    public String getParameterTypeName(int param) throws SQLException {
        try {
            debugCodeCall("getParameterTypeName", param);
            ParameterInterface p = getParameter(param);
            int type = p.getType();
            if (type == Value.UNKNOWN) {
                type = Value.STRING;
            }
            return DataType.getDataType(type).name;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private ParameterInterface getParameter(int param) {
        checkClosed();
        if (param < 1 || param > paramCount) {
            throw DbException.getInvalidValueException("param", param);
        }
        return parameters.get(param - 1);
    }

    private void checkClosed() {
        prep.checkClosed();
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw DbException.getInvalidValueException("iface", iface);
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": parameterCount=" + paramCount;
    }

}
