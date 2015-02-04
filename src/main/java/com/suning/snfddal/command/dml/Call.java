/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.dml;

import java.sql.ResultSet;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.Prepared;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.command.expression.ExpressionVisitor;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.result.LocalResult;
import com.suning.snfddal.result.ResultInterface;
import com.suning.snfddal.value.Value;

/**
 * This class represents the statement
 * CALL.
 */
public class Call extends Prepared {

    private boolean isResultSet;
    private Expression expression;
    private Expression[] expressions;

    public Call(Session session) {
        super(session);
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result;
        if (isResultSet) {
            Expression[] expr = expression.getExpressionColumns(session);
            result = new LocalResult(session, expr, expr.length);
        } else {
            result = new LocalResult(session, expressions, 1);
        }
        result.done();
        return result;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public ResultInterface query(int maxrows) {
        setCurrentRowNumber(1);
        Value v = expression.getValue(session);
        if (isResultSet) {
            v = v.convertTo(Value.RESULT_SET);
            ResultSet rs = v.getResultSet();
            return LocalResult.read(session, rs, maxrows);
        }
        LocalResult result = new LocalResult(session, expressions, 1);
        Value[] row = { v };
        result.addRow(row);
        result.done();
        return result;
    }

    @Override
    public void prepare() {
        expression = expression.optimize(session);
        expressions = new Expression[] { expression };
        isResultSet = expression.getType() == Value.RESULT_SET;
        if (isResultSet) {
            prepareAlways = true;
        }
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return expression.isEverything(ExpressionVisitor.READONLY_VISITOR);

    }

    @Override
    public int getType() {
        return CommandInterface.CALL;
    }

    @Override
    public boolean isCacheable() {
        return !isResultSet;
    }

}
