/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.table;

import java.util.ArrayList;

import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.index.IndexMate;
import com.suning.snfddal.dbobject.index.IndexType;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.value.Value;

/**
 * The table SYSTEM_RANGE is a virtual table that generates incrementing numbers
 * with a given start end end point.
 */
public class RangeTable extends Table {

    /**
     * The name of the range table.
     */
    public static final String NAME = "SYSTEM_RANGE";

    private Expression min, max;
    private boolean optimized;

    /**
     * Create a new range with the given start and end expressions.
     *
     * @param schema the schema (always the main schema)
     * @param min the start expression
     * @param max the end expression
     * @param noColumns whether this table has no columns
     */
    public RangeTable(Schema schema, Expression min, Expression max,
            boolean noColumns) {
        super(schema, 0, NAME);
        Column[] cols = noColumns ? new Column[0] : new Column[] { new Column(
                "X", Value.LONG) };
        this.min = min;
        this.max = max;
        setColumns(cols);
    }

    @Override
    public String getSQL() {
        return NAME + "(" + min.getSQL() + ", " + max.getSQL() + ")";
    }


    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");
    }

    @Override
    public boolean canGetRowCount() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        return Math.max(0, getMax(session) - getMin(session) + 1);
    }

    @Override
    public String getTableType() {
        throw DbException.throwInternalError();
    }

    @Override
    public Index getScanIndex(Session session) {
        return new IndexMate(this, 0, null,IndexColumn.wrap(columns),IndexType.createScan());
    }

    /**
     * Calculate and get the start value of this range.
     *
     * @param session the session
     * @return the start value
     */
    public long getMin(Session session) {
        optimize(session);
        return min.getValue(session).getLong();
    }

    /**
     * Calculate and get the end value of this range.
     *
     * @param session the session
     * @return the end value
     */
    public long getMax(Session session) {
        optimize(session);
        return max.getValue(session).getLong();
    }

    private void optimize(Session s) {
        if (!optimized) {
            min = min.optimize(s);
            max = max.optimize(s);
            optimized = true;
        }
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }


    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public long getRowCountApproximation() {
        return 100;
    }


    @Override
    public boolean isDeterministic() {
        return true;
    }

    @Override
    public void addIndex(ArrayList<Column> list, IndexType indexType) {
        throw DbException.getUnsupportedException("SYSTEM_RANGE");        
    }

}
