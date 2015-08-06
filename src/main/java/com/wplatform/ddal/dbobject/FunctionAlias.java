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
package com.wplatform.ddal.dbobject;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;

import com.wplatform.ddal.command.Parser;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.schema.SchemaObjectBase;
import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.engine.SysProperties;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.util.JdbcUtils;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.SourceCompiler;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.DataType;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueArray;
import com.wplatform.ddal.value.ValueNull;

/**
 * Represents a user-defined function, or alias.
 *
 * @author Thomas Mueller
 * @author Gary Tong
 */
public class FunctionAlias extends SchemaObjectBase {

    private String className;
    private String methodName;
    private String source;
    private JavaMethod[] javaMethods;
    private boolean deterministic;
    private boolean bufferResultSetToLocalTemp = true;

    private FunctionAlias(Schema schema, int id, String name) {
        initSchemaObjectBase(schema, id, name, Trace.FUNCTION);
    }

    /**
     * Create a new alias based on a method name.
     *
     * @param schema                     the schema
     * @param id                         the id
     * @param name                       the name
     * @param javaClassMethod            the class and method name
     * @param force                      create the object even if the class or method does not exist
     * @param bufferResultSetToLocalTemp whether the result should be buffered
     * @return the database object
     */
    public static FunctionAlias newInstance(
            Schema schema, int id, String name, String javaClassMethod,
            boolean force, boolean bufferResultSetToLocalTemp) {
        FunctionAlias alias = new FunctionAlias(schema, id, name);
        int paren = javaClassMethod.indexOf('(');
        int lastDot = javaClassMethod.lastIndexOf('.', paren < 0 ?
                javaClassMethod.length() : paren);
        if (lastDot < 0) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, javaClassMethod);
        }
        alias.className = javaClassMethod.substring(0, lastDot);
        alias.methodName = javaClassMethod.substring(lastDot + 1);
        alias.bufferResultSetToLocalTemp = bufferResultSetToLocalTemp;
        alias.init(force);
        return alias;
    }

    /**
     * Create a new alias based on source code.
     *
     * @param schema                     the schema
     * @param id                         the id
     * @param name                       the name
     * @param source                     the source code
     * @param force                      create the object even if the class or method does not exist
     * @param bufferResultSetToLocalTemp whether the result should be buffered
     * @return the database object
     */
    public static FunctionAlias newInstanceFromSource(
            Schema schema, int id, String name, String source, boolean force,
            boolean bufferResultSetToLocalTemp) {
        FunctionAlias alias = new FunctionAlias(schema, id, name);
        alias.source = source;
        alias.bufferResultSetToLocalTemp = bufferResultSetToLocalTemp;
        alias.init(force);
        return alias;
    }

    private static String getMethodSignature(Method m) {
        StatementBuilder buff = new StatementBuilder(m.getName());
        buff.append('(');
        for (Class<?> p : m.getParameterTypes()) {
            // do not use a space here, because spaces are removed
            // in CreateFunctionAlias.setJavaClassMethod()
            buff.appendExceptFirst(",");
            if (p.isArray()) {
                buff.append(p.getComponentType().getName()).append("[]");
            } else {
                buff.append(p.getName());
            }
        }
        return buff.append(')').toString();
    }

    /**
     * Checks if the given method takes a variable number of arguments. For Java
     * 1.4 and older, false is returned. Example:
     * <pre>
     * public static double mean(double... values)
     * </pre>
     *
     * @param m the method to test
     * @return true if the method takes a variable number of arguments.
     */
    static boolean isVarArgs(Method m) {
        if ("1.5".compareTo(SysProperties.JAVA_SPECIFICATION_VERSION) > 0) {
            return false;
        }
        try {
            Method isVarArgs = m.getClass().getMethod("isVarArgs");
            Boolean result = (Boolean) isVarArgs.invoke(m);
            return result.booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

    private void init(boolean force) {
        try {
            // at least try to compile the class, otherwise the data type is not
            // initialized if it could be
            load();
        } catch (DbException e) {
            if (!force) {
                throw e;
            }
        }
    }

    private synchronized void load() {
        if (javaMethods != null) {
            return;
        }
        if (source != null) {
            loadFromSource();
        } else {
            loadClass();
        }
    }

    private void loadFromSource() {
        SourceCompiler compiler = database.getCompiler();
        synchronized (compiler) {
            String fullClassName = Constants.USER_PACKAGE + "." + getName();
            compiler.setSource(fullClassName, source);
            try {
                Method m = compiler.getMethod(fullClassName);
                JavaMethod method = new JavaMethod(m, 0);
                javaMethods = new JavaMethod[]{
                        method
                };
            } catch (DbException e) {
                throw e;
            } catch (Exception e) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_1, e, source);
            }
        }
    }

    private void loadClass() {
        Class<?> javaClass = JdbcUtils.loadUserClass(className);
        Method[] methods = javaClass.getMethods();
        ArrayList<JavaMethod> list = New.arrayList();
        for (int i = 0, len = methods.length; i < len; i++) {
            Method m = methods[i];
            if (!Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            if (m.getName().equals(methodName) ||
                    getMethodSignature(m).equals(methodName)) {
                JavaMethod javaMethod = new JavaMethod(m, i);
                for (JavaMethod old : list) {
                    if (old.getParameterCount() == javaMethod.getParameterCount()) {
                        throw DbException.get(ErrorCode.
                                        METHODS_MUST_HAVE_DIFFERENT_PARAMETER_COUNTS_2,
                                old.toString(), javaMethod.toString());
                    }
                }
                list.add(javaMethod);
            }
        }
        if (list.size() == 0) {
            throw DbException.get(
                    ErrorCode.PUBLIC_STATIC_JAVA_METHOD_NOT_FOUND_1,
                    methodName + " (" + className + ")");
        }
        javaMethods = new JavaMethod[list.size()];
        list.toArray(javaMethods);
        // Sort elements. Methods with a variable number of arguments must be at
        // the end. Reason: there could be one method without parameters and one
        // with a variable number. The one without parameters needs to be used
        // if no parameters are given.
        Arrays.sort(javaMethods);
    }

    @Override
    public String getSQL() {
        // TODO can remove this method once FUNCTIONS_IN_SCHEMA is enabled
        if (database.getSettings().functionsInSchema ||
                !getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            return super.getSQL();
        }
        return Parser.quoteIdentifier(getName());
    }

    @Override
    public int getType() {
        return DbObject.FUNCTION_ALIAS;
    }

    @Override
    public synchronized void removeChildrenAndResources(Session session) {
        className = null;
        methodName = null;
        javaMethods = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("RENAME");
    }

    /**
     * Find the Java method that matches the arguments.
     *
     * @param args the argument list
     * @return the Java method
     * @throws DbException if no matching method could be found
     */
    public JavaMethod findJavaMethod(Expression[] args) {
        load();
        int parameterCount = args.length;
        for (JavaMethod m : javaMethods) {
            int count = m.getParameterCount();
            if (count == parameterCount || (m.isVarArgs() &&
                    count <= parameterCount + 1)) {
                return m;
            }
        }
        throw DbException.get(ErrorCode.METHOD_NOT_FOUND_1, getName() + " (" +
                className + ", parameter count: " + parameterCount + ")");
    }

    public String getJavaClassName() {
        return this.className;
    }

    public String getJavaMethodName() {
        return this.methodName;
    }

    /**
     * Get the Java methods mapped by this function.
     *
     * @return the Java methods.
     */
    public JavaMethod[] getJavaMethods() {
        load();
        return javaMethods;
    }

    public boolean isDeterministic() {
        return deterministic;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    public String getSource() {
        return source;
    }

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     *
     * @return true if yes
     */
    public boolean isBufferResultSetToLocalTemp() {
        return bufferResultSetToLocalTemp;
    }

    /**
     * There may be multiple Java methods that match a function name.
     * Each method must have a different number of parameters however.
     * This helper class represents one such method.
     */
    public static class JavaMethod implements Comparable<JavaMethod> {
        private final int id;
        private final Method method;
        private final int dataType;
        private boolean hasConnectionParam;
        private boolean varArgs;
        private Class<?> varArgClass;
        private int paramCount;

        JavaMethod(Method method, int id) {
            this.method = method;
            this.id = id;
            Class<?>[] paramClasses = method.getParameterTypes();
            paramCount = paramClasses.length;
            if (paramCount > 0) {
                Class<?> paramClass = paramClasses[0];
                if (Connection.class.isAssignableFrom(paramClass)) {
                    hasConnectionParam = true;
                    paramCount--;
                }
            }
            if (paramCount > 0) {
                Class<?> lastArg = paramClasses[paramClasses.length - 1];
                if (lastArg.isArray() && FunctionAlias.isVarArgs(method)) {
                    varArgs = true;
                    varArgClass = lastArg.getComponentType();
                }
            }
            Class<?> returnClass = method.getReturnType();
            dataType = DataType.getTypeFromClass(returnClass);
        }

        @Override
        public String toString() {
            return method.toString();
        }

        /**
         * Check if this function requires a database connection.
         *
         * @return if the function requires a connection
         */
        public boolean hasConnectionParam() {
            return this.hasConnectionParam;
        }

        /**
         * Call the user-defined function and return the value.
         *
         * @param session    the session
         * @param args       the argument list
         * @param columnList true if the function should only return the column
         *                   list
         * @return the value
         */
        public Value getValue(Session session, Expression[] args,
                              boolean columnList) {
            Class<?>[] paramClasses = method.getParameterTypes();
            Object[] params = new Object[paramClasses.length];
            int p = 0;
            if (hasConnectionParam && params.length > 0) {
                params[p++] = session.createConnection(columnList);
            }

            // allocate array for varArgs parameters
            Object varArg = null;
            if (varArgs) {
                int len = args.length - params.length + 1 +
                        (hasConnectionParam ? 1 : 0);
                varArg = Array.newInstance(varArgClass, len);
                params[params.length - 1] = varArg;
            }

            for (int a = 0, len = args.length; a < len; a++, p++) {
                boolean currentIsVarArg = varArgs &&
                        p >= paramClasses.length - 1;
                Class<?> paramClass;
                if (currentIsVarArg) {
                    paramClass = varArgClass;
                } else {
                    paramClass = paramClasses[p];
                }
                int type = DataType.getTypeFromClass(paramClass);
                Value v = args[a].getValue(session);
                Object o;
                if (Value.class.isAssignableFrom(paramClass)) {
                    o = v;
                } else if (v.getType() == Value.ARRAY &&
                        paramClass.isArray() &&
                        paramClass.getComponentType() != Object.class) {
                    Value[] array = ((ValueArray) v).getList();
                    Object[] objArray = (Object[]) Array.newInstance(
                            paramClass.getComponentType(), array.length);
                    int componentType = DataType.getTypeFromClass(
                            paramClass.getComponentType());
                    for (int i = 0; i < objArray.length; i++) {
                        objArray[i] = array[i].convertTo(componentType).getObject();
                    }
                    o = objArray;
                } else {
                    v = v.convertTo(type);
                    o = v.getObject();
                }
                if (o == null) {
                    if (paramClass.isPrimitive()) {
                        if (columnList) {
                            // If the column list is requested, the parameters
                            // may be null. Need to set to default value,
                            // otherwise the function can't be called at all.
                            o = DataType.getDefaultForPrimitiveType(paramClass);
                        } else {
                            // NULL for a java primitive: return NULL
                            return ValueNull.INSTANCE;
                        }
                    }
                } else {
                    if (!paramClass.isAssignableFrom(o.getClass()) && !paramClass.isPrimitive()) {
                        o = DataType.convertTo(session.createConnection(false), v, paramClass);
                    }
                }
                if (currentIsVarArg) {
                    Array.set(varArg, p - params.length + 1, o);
                } else {
                    params[p] = o;
                }
            }
            boolean old = session.getAutoCommit();
            Value identity = session.getLastScopeIdentity();
            try {
                session.setAutoCommit(false);
                Object returnValue;
                try {
                    returnValue = method.invoke(null, params);
                    if (returnValue == null) {
                        return ValueNull.INSTANCE;
                    }
                } catch (InvocationTargetException e) {
                    StatementBuilder buff = new StatementBuilder(method.getName());
                    buff.append('(');
                    for (Object o : params) {
                        buff.appendExceptFirst(", ");
                        buff.append(o == null ? "null" : o.toString());
                    }
                    buff.append(')');
                    throw DbException.convertInvocation(e, buff.toString());
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
                if (Value.class.isAssignableFrom(method.getReturnType())) {
                    return (Value) returnValue;
                }
                Value ret = DataType.convertToValue(session, returnValue, dataType);
                return ret.convertTo(dataType);
            } finally {
                session.setLastScopeIdentity(identity);
                session.setAutoCommit(old);
            }
        }

        public Class<?>[] getColumnClasses() {
            return method.getParameterTypes();
        }

        public int getDataType() {
            return dataType;
        }

        public int getParameterCount() {
            return paramCount;
        }

        public boolean isVarArgs() {
            return varArgs;
        }

        @Override
        public int compareTo(JavaMethod m) {
            if (varArgs != m.varArgs) {
                return varArgs ? 1 : -1;
            }
            if (paramCount != m.paramCount) {
                return paramCount - m.paramCount;
            }
            if (hasConnectionParam != m.hasConnectionParam) {
                return hasConnectionParam ? 1 : -1;
            }
            return id - m.id;
        }

    }

}
