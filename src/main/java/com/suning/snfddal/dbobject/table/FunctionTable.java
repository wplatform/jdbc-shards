/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.table;

import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.command.expression.FunctionCall;
import com.suning.snfddal.command.expression.TableFunction;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.index.IndexMate;
import com.suning.snfddal.dbobject.index.IndexType;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.result.LocalResult;
import com.suning.snfddal.result.ResultInterface;
import com.suning.snfddal.value.DataType;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueNull;
import com.suning.snfddal.value.ValueResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * A table backed by a system or user-defined function that returns a result
 * set.
 */
public class FunctionTable extends Table {

    private final FunctionCall function;
    private final long rowCount;
    private Expression functionExpr;
    private LocalResult cachedResult;
    private Value cachedValue;

    public FunctionTable(Schema schema, Session session,
                         Expression functionExpr, FunctionCall function) {
        super(schema, 0, function.getName());
        this.functionExpr = functionExpr;
        this.function = function;
        if (function instanceof TableFunction) {
            rowCount = ((TableFunction) function).getRowCount();
        } else {
            rowCount = Long.MAX_VALUE;
        }
        function.optimize(session);
        int type = function.getType();
        if (type != Value.RESULT_SET) {
            throw DbException.get(
                    ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        Expression[] args = function.getArgs();
        int numParams = args.length;
        Expression[] columnListArgs = new Expression[numParams];
        for (int i = 0; i < numParams; i++) {
            args[i] = args[i].optimize(session);
            columnListArgs[i] = args[i];
        }
        ValueResultSet template = function.getValueForColumnList(
                session, columnListArgs);
        if (template == null) {
            throw DbException.get(
                    ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName());
        }
        ResultSet rs = template.getResultSet();
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            Column[] cols = new Column[columnCount];
            for (int i = 0; i < columnCount; i++) {
                cols[i] = new Column(meta.getColumnName(i + 1),
                        DataType.getValueTypeFromResultSet(meta, i + 1),
                        meta.getPrecision(i + 1),
                        meta.getScale(i + 1), meta.getColumnDisplaySize(i + 1));
            }
            setColumns(cols);
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }


    @Override
    public String getTableType() {
        return null;
    }

    @Override
    public Index getScanIndex(Session session) {
        return new IndexMate(this, 0, null, IndexColumn.wrap(columns), IndexType.createScan());
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public boolean canGetRowCount() {
        return rowCount != Long.MAX_VALUE;
    }

    @Override
    public long getRowCount(Session session) {
        return rowCount;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("ALIAS");
    }

    /**
     * Read the result from the function. This method buffers the result in a
     * temporary file.
     *
     * @param session the session
     * @return the result
     */
    public ResultInterface getResult(Session session) {
        ValueResultSet v = getValueResultSet(session);
        if (v == null) {
            return null;
        }
        if (cachedResult != null && cachedValue == v) {
            cachedResult.reset();
            return cachedResult;
        }
        LocalResult result = LocalResult.read(session, v.getResultSet(), 0);
        if (function.isDeterministic()) {
            cachedResult = result;
            cachedValue = v;
        }
        return result;
    }

    /**
     * Read the result set from the function. This method doesn't cache.
     *
     * @param session the session
     * @return the result set
     */
    public ResultSet getResultSet(Session session) {
        ValueResultSet v = getValueResultSet(session);
        return v == null ? null : v.getResultSet();
    }

    private ValueResultSet getValueResultSet(Session session) {
        functionExpr = functionExpr.optimize(session);
        Value v = functionExpr.getValue(session);
        if (v == ValueNull.INSTANCE) {
            return null;
        }
        return (ValueResultSet) v;
    }

    public boolean isBufferResultSetToLocalTemp() {
        return function.isBufferResultSetToLocalTemp();
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public String getSQL() {
        return function.getSQL();
    }

    @Override
    public long getRowCountApproximation() {
        return rowCount;
    }

    @Override
    public boolean isDeterministic() {
        return function.isDeterministic();
    }

}
