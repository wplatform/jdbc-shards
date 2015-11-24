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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;

import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.RoutingHandler;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.support.GeneralJdbcOperations;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.result.ResultInterface;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class CommonPreparedExecutor<T extends Prepared> implements PreparedExecutor {

    protected T prepared;
    protected Session session;
    protected Database database;
    protected ThreadPoolExecutor sqlExecutor;
    protected RoutingHandler routingHandler;
    protected int maxrows;

    /**
     * @param session
     * @param prepared
     */
    public CommonPreparedExecutor(Session session, T prepared) {
        super();
        this.prepared = prepared;
        this.session = session;
        this.database = session.getDatabase();
    }

    public JdbcOperations getJdbcOperations(String shardName) {
        ConcurrentMap<String, JdbcOperations> map = session.getJdbcOperationsMap();
        return map.putIfAbsent(shardName, new GeneralJdbcOperations(shardName, session));
    }

    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    @Override
    public ResultInterface executeQuery(int maxrows) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    @Override
    public int executeUpdate() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Check if this object is a query.
     *
     * @return true if it is
     */
    public boolean isQuery() {
        return false;
    }

    protected T getPrepared() {
        return this.prepared;
    }

    public int getMaxrows() {
        return maxrows;
    }

    public void setMaxrows(int maxrows) {
        this.maxrows = maxrows;
    }

    /**
     * @param tableName
     */
    public TableMate getTableMate(String tableName) {
        Table table = database.getSchema(session.getCurrentSchemaName()).findTableOrView(session, tableName);
        if (table == null) {
            String[] schemaNames = session.getSchemaSearchPath();
            if (schemaNames != null) {
                for (String name : schemaNames) {
                    Schema s = database.getSchema(name);
                    table = s.findTableOrView(session, tableName);
                    if (table != null) {
                        break;
                    }
                }
            }
        }
        if (table != null && table instanceof TableMate) {
            return (TableMate) table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }    
    
    protected Worker<UpdateResult> createUpdateWorker(String shardName, String sql, List<Value> params) {
        return new UpdateWorker(shardName, sql, params);
    }
    
    protected Worker<QueryResult> createQueryWorker(String shardName, String sql, List<Value> params) {
        return new QueryWorker(shardName, sql, params);
    }
    
    protected Worker<UpdateResult> createBatchUpdateWorker(String shardName, String sql, List<Value>[] array) {
        return new BatchUpdateWorker(shardName, sql, array);
    }
    
    protected static abstract class Worker<T> implements Callable<T> {
        protected final String shardName;
        protected final String sql;
        protected final List<Value> params;
        public Worker(String shardName, String sql, List<Value> params) {
            super();
            this.shardName = shardName;
            this.sql = sql;
            this.params = params;
        }
        public abstract T doWork();
        public T call() throws Exception {
            return doWork();
        }
    }

    protected class UpdateWorker extends Worker<UpdateResult> {
        public UpdateWorker(String shardName, String sql, List<Value> params) {
            super(shardName, sql, params);
        }
        @Override
        public UpdateResult doWork() {
            return getJdbcOperations(shardName).executeUpdate(sql, params);
        }
    }

    protected class QueryWorker extends Worker<QueryResult> {
        public QueryWorker(String shardName, String sql, List<Value> params) {
            super(shardName, sql, params);
        }
        @Override
        public QueryResult doWork() {
            int maxRows = maxrows > 0 ? maxrows:database.getMaxMemoryRows();
            return getJdbcOperations(shardName).executeQuery(sql, params, maxRows);
        }
    }
    
    protected class BatchUpdateWorker extends Worker<UpdateResult> {
        protected final List<Value>[] array;
        public BatchUpdateWorker(String shardName, String sql, List<Value>[] array) {
            super(shardName, sql, null);
            this.array = array;
        }
        @Override
        public UpdateResult doWork() {
            return getJdbcOperations(shardName).batchUpdate(sql, array);
        }
    }

}
