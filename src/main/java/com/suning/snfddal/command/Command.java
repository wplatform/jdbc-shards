/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command;

import com.suning.snfddal.command.expression.ParameterInterface;
import com.suning.snfddal.engine.Constants;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.result.ResultInterface;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Represents a SQL statement. This object is only used on the server side.
 */
public abstract class Command implements CommandInterface {

    /**
     * The session.
     */
    protected final Session session;
    /**
     * The trace module.
     */
    private final Trace trace;
    private final String sql;
    /**
     * The last start time.
     */
    protected long startTime;
    /**
     * If this query was canceled.
     */
    private volatile boolean cancel;
    private boolean canReuse;

    Command(Parser parser, String sql) {
        this.session = parser.getSession();
        this.sql = sql;
        trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    /**
     * Check if this command is transactional.
     * If it is not, then it forces the current transaction to commit.
     *
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Check if this command is a query.
     *
     * @return true if it is
     */
    @Override
    public abstract boolean isQuery();

    /**
     * Get the list of parameters.
     *
     * @return the list of parameters
     */
    @Override
    public abstract ArrayList<? extends ParameterInterface> getParameters();

    /**
     * Check if this command is read only.
     *
     * @return true if it is
     */
    public abstract boolean isReadOnly();

    /**
     * Get an empty result set containing the meta data.
     *
     * @return an empty result set
     */
    public abstract ResultInterface queryMeta();

    /**
     * Execute an updating statement (for example insert, delete, or update), if
     * this is possible.
     *
     * @return the update count
     * @throws DbException if the command is not an updating statement
     */
    public int update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute a query statement, if this is possible.
     *
     * @param maxrows the maximum number of rows returned
     * @return the local result set
     * @throws DbException if the command is not a query
     */
    public ResultInterface query(int maxrows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    @Override
    public final ResultInterface getMetaData() {
        return queryMeta();
    }

    /**
     * Start the stopwatch.
     */
    void start() {
        if (trace.isInfoEnabled()) {
            startTime = System.currentTimeMillis();
        }
    }

    /**
     * Check if this command has been canceled, and throw an exception if yes.
     *
     * @throws DbException if the statement has been canceled
     */
    protected void checkCanceled() {
        if (cancel) {
            cancel = false;
            throw DbException.get(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    private void stop() {
        session.endStatement();
        session.setCurrentCommand(null);
        if (!isTransactional()) {
            session.commit(true);
        } else if (session.getAutoCommit()) {
            session.commit(false);
        }
        if (trace.isInfoEnabled() && startTime > 0) {
            long time = System.currentTimeMillis() - startTime;
            if (time > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: {0} ms", time);
            }
        }
    }

    /**
     * Execute a query and return the result.
     * This method prepares everything and calls {@link #query(int)} finally.
     *
     * @param maxrows    the maximum number of rows to return
     * @param scrollable if the result set must be scrollable (ignored)
     * @return the result set
     */
    @Override
    public ResultInterface executeQuery(int maxrows, boolean scrollable) {
        startTime = 0;
        Database database = session.getDatabase();
        Object sync = session;
        boolean callStop = true;
        synchronized (sync) {
            session.setCurrentCommand(this);
            try {
                while (true) {
                    try {
                        return query(maxrows);
                    } catch (DbException e) {
                        throw e;
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        // there is a serious problem:
                        // the transaction may be applied partially
                        // in this case we need to panic:
                        // close the database
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                throw e;
            } finally {
                if (callStop) {
                    stop();
                }
            }
        }
    }

    @Override
    public int executeUpdate() {
        Database database = session.getDatabase();
        Object sync = session;
        boolean callStop = true;
        synchronized (sync) {
            Session.Savepoint rollback = session.setSavepoint();
            session.setCurrentCommand(this);
            try {
                while (true) {
                    try {
                        return update();
                    } catch (DbException e) {
                        throw e;
                    } catch (OutOfMemoryError e) {
                        callStop = false;
                        database.shutdownImmediately();
                        throw DbException.convert(e);
                    } catch (Throwable e) {
                        throw DbException.convert(e);
                    }
                }
            } catch (DbException e) {
                e = e.addSQL(sql);
                SQLException s = e.getSQLException();
                if (s.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                    callStop = false;
                    database.shutdownImmediately();
                    throw e;
                }
                if (s.getErrorCode() == ErrorCode.DEADLOCK_1) {
                    session.rollback();
                } else {
                    session.rollbackTo(rollback, false);
                }
                throw e;
            } finally {
                try {
                    if (callStop) {
                        stop();
                    }
                } finally {
                }
            }
        }
    }

    @Override
    public void close() {
        canReuse = true;
    }

    @Override
    public void cancel() {
        this.cancel = true;
    }

    @Override
    public String toString() {
        return sql + Trace.formatParams(getParameters());
    }

    public boolean isCacheable() {
        return false;
    }

    /**
     * Whether the command is already closed (in which case it can be re-used).
     *
     * @return true if it can be re-used
     */
    public boolean canReuse() {
        return canReuse;
    }

    /**
     * The command is now re-used, therefore reset the canReuse flag, and the
     * parameter values.
     */
    public void reuse() {
        canReuse = false;
        ArrayList<? extends ParameterInterface> parameters = getParameters();
        for (int i = 0, size = parameters.size(); i < size; i++) {
            ParameterInterface param = parameters.get(i);
            param.setValue(null, true);
        }
    }

}
