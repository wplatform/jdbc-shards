/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.constraint;

import java.util.HashSet;
import java.util.Iterator;

import com.suning.snfddal.api.ErrorCode;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.command.expression.ExpressionVisitor;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.Row;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StringUtils;

/**
 * A check constraint.
 */
public class ConstraintCheck extends Constraint {

    private TableFilter filter;
    private Expression expr;

    public ConstraintCheck(Schema schema, int id, String name, Table table) {
        super(schema, id, name, table);
    }

    @Override
    public String getConstraintType() {
        return Constraint.CHECK;
    }

    public void setTableFilter(TableFilter filter) {
        this.filter = filter;
    }

    public void setExpression(Expression expr) {
        this.expr = expr;
    }

    @Override
    public String getCreateSQLForCopy(Table forTable, String quotedName) {
        StringBuilder buff = new StringBuilder("ALTER TABLE ");
        buff.append(forTable.getSQL()).append(" ADD CONSTRAINT ");
        if (forTable.isHidden()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append(" CHECK").append(StringUtils.enclose(expr.getSQL()))
                .append(" NOCHECK");
        return buff.toString();
    }

    private String getShortDescription() {
        return getName() + ": " + expr.getSQL();
    }

    @Override
    public String  getCreateSQLWithoutIndexes() {
        return getCreateSQL();
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQLForCopy(table, getSQL());
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        table.removeConstraint(this);
        filter = null;
        expr = null;
        table = null;
        invalidate();
    }

    @Override
    public void checkRow(Session session, Table t, Row oldRow, Row newRow) {
        if (newRow == null) {
            return;
        }
        filter.set(newRow);
        Boolean b;
        try {
            b = expr.getValue(session).getBoolean();
        } catch (DbException ex) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID, ex,
                    getShortDescription());
        }
        // Both TRUE and NULL are ok
        if (Boolean.FALSE.equals(b)) {
            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_VIOLATED_1,
                    getShortDescription());
        }
    }

    @Override
    public boolean usesIndex(Index index) {
        return false;
    }

    @Override
    public void setIndexOwner(Index index) {
        DbException.throwInternalError();
    }

    @Override
    public HashSet<Column> getReferencedColumns(Table table) {
        HashSet<Column> columns = New.hashSet();
        expr.isEverything(ExpressionVisitor.getColumnsVisitor(columns));
        for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
            if (it.next().getTable() != table) {
                it.remove();
            }
        }
        return columns;
    }

    public Expression getExpression() {
        return expr;
    }

    @Override
    public boolean isBefore() {
        return true;
    }

    @Override
    public void checkExistingData(Session session) {
        
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    @Override
    public void rebuild() {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

}
