/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.expression;

import java.util.List;

import com.suning.snfddal.dbobject.index.IndexCondition;
import com.suning.snfddal.dbobject.table.ColumnResolver;
import com.suning.snfddal.dbobject.table.TableFilter;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.value.Value;
import com.suning.snfddal.value.ValueArray;
import com.suning.snfddal.value.ValueBoolean;
import com.suning.snfddal.value.ValueNull;

/**
 * An expression representing a constant value.
 */
public class ValueExpression extends Expression {
    /**
     * The expression represents ValueNull.INSTANCE.
     */
    private static final Object NULL = new ValueExpression(ValueNull.INSTANCE);

    /**
     * This special expression represents the default value. It is used for
     * UPDATE statements of the form SET COLUMN = DEFAULT. The value is
     * ValueNull.INSTANCE, but should never be accessed.
     */
    private static final Object DEFAULT = new ValueExpression(ValueNull.INSTANCE);

    private final Value value;

    private ValueExpression(Value value) {
        this.value = value;
    }

    /**
     * Get the NULL expression.
     *
     * @return the NULL expression
     */
    public static ValueExpression getNull() {
        return (ValueExpression) NULL;
    }

    /**
     * Get the DEFAULT expression.
     *
     * @return the DEFAULT expression
     */
    public static ValueExpression getDefault() {
        return (ValueExpression) DEFAULT;
    }

    /**
     * Create a new expression with the given value.
     *
     * @param value the value
     * @return the expression
     */
    public static ValueExpression get(Value value) {
        if (value == ValueNull.INSTANCE) {
            return getNull();
        }
        return new ValueExpression(value);
    }

    @Override
    public Value getValue(Session session) {
        return value;
    }

    @Override
    public int getType() {
        return value.getType();
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (value.getType() == Value.BOOLEAN) {
            boolean v = ((ValueBoolean) value).getBoolean().booleanValue();
            if (!v) {
                filter.addIndexCondition(IndexCondition.get(Comparison.FALSE, null, this));
            }
        }
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.get(false)));
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isValueSet() {
        return true;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    @Override
    public int getScale() {
        return value.getScale();
    }

    @Override
    public long getPrecision() {
        return value.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return value.getDisplaySize();
    }

    @Override
    public String getSQL() {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        return value.getSQL();
    }

    @Override
    public void updateAggregate(Session session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        default:
            throw DbException.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        if (getType() == Value.ARRAY) {
            return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }
    
    @Override
    public String exportParameters(TableFilter filter, List<Value> container) {
        if (this == DEFAULT) {
            return "DEFAULT";
        }
        container.add(value);
        return "?";
    }
}
