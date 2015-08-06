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
package com.wplatform.ddal.command.dml;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.ResultInterface;

/**
 * Represents a transactional statement.
 */
public class TransactionCommand extends Prepared {

    private final int type;
    private String savepointName;
    private String transactionName;

    public TransactionCommand(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setSavepointName(String name) {
        this.savepointName = name;
    }

    @Override
    public int update() {
        switch (type) {
            case CommandInterface.SET_AUTOCOMMIT_TRUE:
                session.setAutoCommit(true);
                break;
            case CommandInterface.SET_AUTOCOMMIT_FALSE:
                session.setAutoCommit(false);
                break;
            case CommandInterface.BEGIN:
                session.begin();
                break;
            case CommandInterface.COMMIT:
                session.commit(false);
                break;
            case CommandInterface.ROLLBACK:
                session.rollback();
                break;
            case CommandInterface.CHECKPOINT:
                session.getUser().checkAdmin();
                //session.getDatabase().checkpoint();
                break;
            case CommandInterface.SAVEPOINT:
                session.addSavepoint(savepointName);
                break;
            case CommandInterface.ROLLBACK_TO_SAVEPOINT:
                session.rollbackToSavepoint(savepointName);
                break;
            case CommandInterface.CHECKPOINT_SYNC:
                session.getUser().checkAdmin();
                //session.getDatabase().sync();
                break;
            case CommandInterface.PREPARE_COMMIT:
                session.prepareCommit(transactionName);
                break;
            case CommandInterface.COMMIT_TRANSACTION:
                session.getUser().checkAdmin();
                session.setPreparedTransaction(transactionName, true);
                break;
            case CommandInterface.ROLLBACK_TRANSACTION:
                session.getUser().checkAdmin();
                session.setPreparedTransaction(transactionName, false);
                break;
            case CommandInterface.SHUTDOWN_IMMEDIATELY:
                session.getUser().checkAdmin();
                session.getDatabase().shutdownImmediately();
                break;
            case CommandInterface.SHUTDOWN:
            case CommandInterface.SHUTDOWN_COMPACT:
            case CommandInterface.SHUTDOWN_DEFRAG: {
                session.getUser().checkAdmin();
                session.commit(false);
                if (type == CommandInterface.SHUTDOWN_COMPACT ||
                        type == CommandInterface.SHUTDOWN_DEFRAG) {
                }
                Database db = session.getDatabase();
                // throttle, to allow testing concurrent
                // execution of shutdown and query
                session.throttle();
                for (Session s : db.getSessions()) {
                    s.rollback();
                    if (s != session) {
                        s.close();
                    }
                }
                session.close();
                break;
            }
            default:
                DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setTransactionName(String string) {
        this.transactionName = string;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

}
