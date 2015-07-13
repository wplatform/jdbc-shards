/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.engine;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.value.Value;

import java.io.Closeable;

/**
 * A local or remote session. A session represents a database connection.
 */
public interface SessionInterface extends Closeable {
    /**
     * Parse a command and prepare it for execution.
     *
     * @param sql       the SQL statement
     * @param fetchSize the number of rows to fetch in one step
     * @return the prepared command
     */
    CommandInterface prepareCommand(String sql, int fetchSize);

    /**
     * Roll back pending transactions and close the session.
     */
    @Override
    void close();

    /**
     * Get the trace object
     *
     * @return the trace object
     */
    Trace getTrace();

    /**
     * Check if close was called.
     *
     * @return if the session has been closed
     */
    boolean isClosed();

    /**
     * Check whether this session has a pending transaction.
     *
     * @return true if it has
     */
    boolean hasPendingTransaction();

    /**
     * Cancel the current or next command (called when closing a connection).
     */
    void cancel();

    /**
     * Check if the database changed and therefore reconnecting is required.
     *
     * @param write if the next operation may be writing
     * @return true if reconnecting is required
     */
    boolean isReconnectNeeded(boolean write);

    /**
     * Close the connection and open a new connection.
     *
     * @param write if the next operation may be writing
     * @return the new connection
     */
    SessionInterface reconnect(boolean write);

    /**
     * Check if this session is in auto-commit mode.
     *
     * @return true if the session is in auto-commit mode
     */
    boolean getAutoCommit();

    /**
     * Set the auto-commit mode. This call doesn't commit the current
     * transaction.
     *
     * @param autoCommit the new value
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Add a temporary LOB, which is closed when the session commits.
     *
     * @param v the value
     */
    void addTemporaryLob(Value v);

    /**
     * Get transaction isolation level
     *
     * @return return the transaction isolation
     */
    int getTransactionIsolation();

    /**
     * Set the transaction isolation level for the current transaction.
     *
     * @param level isolation value
     */
    void setTransactionIsolation(int level);

    /**
     * Check if this session is in read-only mode.
     *
     * @return true if the session is in read-only mode.
     */
    boolean isReadOnly();

    /**
     * Set the read-only mode.
     *
     * @param readOnly the new value
     */
    void setReadOnly(boolean readOnly);

}
