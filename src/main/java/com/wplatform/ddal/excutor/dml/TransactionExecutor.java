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
package com.wplatform.ddal.excutor.dml;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.dml.TransactionCommand;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.message.DbException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class TransactionExecutor extends CommonPreparedExecutor<TransactionCommand> {

    /**
     * @param prepared
     */
    public TransactionExecutor(TransactionCommand prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        int type = prepared.getType();
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
        case CommandInterface.SAVEPOINT:
            session.addSavepoint(prepared.getSavepointName());
            break;
        case CommandInterface.ROLLBACK_TO_SAVEPOINT:
            session.rollbackToSavepoint(prepared.getSavepointName());
            break;
        case CommandInterface.PREPARE_COMMIT:
            session.prepareCommit(prepared.getTransactionName());
            break;
        case CommandInterface.COMMIT_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(prepared.getTransactionName(), true);
            break;
        case CommandInterface.ROLLBACK_TRANSACTION:
            session.getUser().checkAdmin();
            session.setPreparedTransaction(prepared.getTransactionName(), false);
            break;
        case CommandInterface.TRANSACTION_ISOLATION:
            session.getUser().checkAdmin();
            Expression expr = prepared.getExpression();
            expr = expr.optimize(session);
            session.setTransactionIsolation(expr.getValue(session).getInt());
            break;
        case CommandInterface.TRANSACTION_READONLY_FALSE:
            session.getUser().checkAdmin();
            session.setReadOnly(false);
            break;
        case CommandInterface.TRANSACTION_READONLY_TRUE:
            session.getUser().checkAdmin();
            session.setReadOnly(true);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

}
