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
// Created on 2015年4月12日
// $Id$

package com.wplatform.ddal.excutor;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.dispatch.RoutingHandler;
import com.wplatform.ddal.dispatch.rule.GroupTableNode;
import com.wplatform.ddal.dispatch.rule.RoutingResult;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.ResultInterface;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class CommonPreparedExecutor<T extends Prepared> implements PreparedExecutor<T> {

    protected T prepared;
    protected Session session;
    protected ExecutorService executorService;
    protected RoutingHandler routingHandler;

    /**
     * @param session
     * @param prepared
     */
    public CommonPreparedExecutor(Session session, T prepared) {
        super();
        this.prepared = prepared;
        this.session = session;
    }

    /**
     * Wrap a SQL exception that occurred while accessing a linked table.
     *
     * @param sql the SQL statement
     * @param ex  the exception from the remote database
     * @return the wrapped exception
     */
    protected static DbException wrapException(String sql, Exception ex) {
        SQLException e = DbException.toSQLException(ex);
        return DbException.get(ErrorCode.ERROR_ACCESSING_DATABASE_TABLE_2, e, sql, e.toString());
    }

    @Override
    public ResultInterface executeQuery(int maxrows) {
        switch (prepared.getType()) {
            case CommandInterface.SELECT:
            case CommandInterface.CALL:
                RoutingResult rr = doRoute();
                TableNode[] tableNodes = rr.group();
                for (TableNode tableNode : tableNodes) {
                    if (tableNode instanceof GroupTableNode) {
                        doTranslate((GroupTableNode) tableNode);
                    } else {
                        doTranslate(tableNode);
                    }
                }

                throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
            default:
                throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
        }

    }

    @Override
    public int executeUpdate() {
        switch (prepared.getType()) {
            case CommandInterface.SELECT:
            case CommandInterface.CALL:
                throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
            default:
                RoutingResult rr = doRoute();
                TableNode[] tableNodes = rr.group();
                for (TableNode tableNode : tableNodes) {
                    if (tableNode instanceof GroupTableNode) {
                        doTranslate((GroupTableNode) tableNode);
                    } else {
                        doTranslate(tableNode);
                    }
                }
                return 0;
        }
    }

    protected T getPrepared() {
        return this.prepared;
    }

    protected abstract RoutingResult doRoute();

    protected abstract String doTranslate(TableNode tableNode);

    protected abstract String doTranslate(GroupTableNode tableNode);


}
