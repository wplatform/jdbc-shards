package com.suning.snfddal.shards;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class SmartStatement extends SmartSupport implements InvocationHandler {

    private Statement statement;

    /**
     * @param database
     * @param dataSource
     * @param optional
     */
    protected SmartStatement(SmartSupport parent, Statement stmt) {
        super(parent.database, parent.dataSource);
        this.statement = stmt;
    }

    /**
     * Creates a logging version of a Statement
     *
     * @param stmt - the statement
     * @return - the proxy
     */
    public static Statement newInstance(SmartSupport parent, Statement stmt) {
        InvocationHandler handler = new SmartStatement(parent, stmt);
        ClassLoader cl = Statement.class.getClassLoader();
        return (Statement) Proxy.newProxyInstance(cl, new Class[]{Statement.class}, handler);
    }

    public Object invoke(Object proxy, Method method, Object[] params) throws Throwable {
        try {
            if (EXECUTE_METHODS.contains(method.getName())) {
                if (isDebugEnabled()) {
                    debug("==>  Executing: " + removeBreakingWhitespace((String) params[0]));
                }
                if ("executeQuery".equals(method.getName())) {
                    ResultSet rs = (ResultSet) method.invoke(statement, params);
                    if (rs != null) {
                        return SmartResultSet.newInstance(this, rs);
                    } else {
                        return null;
                    }
                } else {
                    return method.invoke(statement, params);
                }
            } else if ("getResultSet".equals(method.getName())) {
                ResultSet rs = (ResultSet) method.invoke(statement, params);
                if (rs != null) {
                    return SmartResultSet.newInstance(this, rs);
                } else {
                    return null;
                }
            } else if ("equals".equals(method.getName())) {
                Object ps = params[0];
                return ps instanceof Proxy && proxy == ps;
            } else if ("hashCode".equals(method.getName())) {
                return proxy.hashCode();
            } else {
                return method.invoke(statement, params);
            }
        } catch (Throwable t) {
            t = unwrapThrowable(t);
            handleException(t);
            throw t;
        }
    }

    /**
     * return the wrapped statement
     *
     * @return the statement
     */
    public Statement getStatement() {
        return statement;
    }

}
