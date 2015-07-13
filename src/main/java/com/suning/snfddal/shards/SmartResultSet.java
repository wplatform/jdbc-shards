package com.suning.snfddal.shards;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class SmartResultSet extends SmartSupport implements InvocationHandler {

    private static Set<Integer> BLOB_TYPES = new HashSet<Integer>();

    static {
        BLOB_TYPES.add(Types.BINARY);
        BLOB_TYPES.add(Types.BLOB);
        BLOB_TYPES.add(Types.CLOB);
        BLOB_TYPES.add(Types.LONGNVARCHAR);
        BLOB_TYPES.add(Types.LONGVARBINARY);
        BLOB_TYPES.add(Types.LONGVARCHAR);
        BLOB_TYPES.add(Types.NCLOB);
        BLOB_TYPES.add(Types.VARBINARY);
    }

    private boolean first = true;
    private int rows = 0;
    private ResultSet rs;
    private Set<Integer> blobColumns = new HashSet<Integer>();

    /**
     * @param database
     * @param dataSource
     * @param optional
     */
    protected SmartResultSet(SmartSupport parent, ResultSet rs) {
        super(parent.database, parent.dataSource);
        this.rs = rs;
    }

    /* Creates a logging version of a ResultSet
     * @param rs - the ResultSet to proxy
     * @return - the ResultSet with logging */
    public static ResultSet newInstance(SmartSupport parent, ResultSet rs) {
        InvocationHandler handler = new SmartResultSet(parent, rs);
        ClassLoader cl = ResultSet.class.getClassLoader();
        return (ResultSet) Proxy.newProxyInstance(cl, new Class[]{ResultSet.class}, handler);
    }

    public Object invoke(Object proxy, Method method, Object[] params) throws Throwable {
        try {
            Object o = method.invoke(rs, params);
            if ("next".equals(method.getName())) {
                if (((Boolean) o)) {
                    rows++;
                    if (isDebugEnabled()) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        final int columnCount = rsmd.getColumnCount();
                        if (first) {
                            first = false;
                            printColumnHeaders(rsmd, columnCount);
                        }
                        printColumnValues(columnCount);
                    }
                } else {
                    debug("<==      Total: " + rows);
                }
            }
            clearColumnInfo();
            return o;
        } catch (Throwable t) {
            t = unwrapThrowable(t);
            handleException(t);
            throw t;
        }
    }

    private void printColumnHeaders(ResultSetMetaData rsmd, int columnCount) throws SQLException {
        StringBuilder row = new StringBuilder();
        row.append("<==    Columns: ");
        for (int i = 1; i <= columnCount; i++) {
            if (BLOB_TYPES.contains(rsmd.getColumnType(i))) {
                blobColumns.add(i);
            }
            String colname = rsmd.getColumnLabel(i);
            row.append(colname);
            if (i != columnCount)
                row.append(", ");
        }
        debug(row.toString());
    }

    private void printColumnValues(int columnCount) throws SQLException {
        StringBuilder row = new StringBuilder();
        row.append("<==        Row: ");
        for (int i = 1; i <= columnCount; i++) {
            String colname;
            try {
                if (blobColumns.contains(i)) {
                    colname = "<<BLOB>>";
                } else {
                    colname = rs.getString(i);
                }
            } catch (SQLException e) {
                // generally can't call getString() on a BLOB column
                colname = "<<Cannot Display>>";
            }
            row.append(colname);
            if (i != columnCount)
                row.append(", ");
        }
        debug(row.toString());
    }

    /* Get the wrapped result set
     * @return the resultSet */
    public ResultSet getRs() {
        return rs;
    }

}
