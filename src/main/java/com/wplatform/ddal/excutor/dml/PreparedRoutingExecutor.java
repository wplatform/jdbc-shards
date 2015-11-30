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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.wplatform.ddal.command.Prepared;
import com.wplatform.ddal.dbobject.index.IndexCondition;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.RoutingHandler;
import com.wplatform.ddal.dispatch.rule.RoutingResult;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.excutor.JdbcWorker;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class PreparedRoutingExecutor<T extends Prepared> extends CommonPreparedExecutor<T> {

    protected final RoutingHandler routingHandler;

    /**
     * @param prepared
     */
    public PreparedRoutingExecutor(T prepared) {
        super(prepared);
        this.routingHandler = database.getRoutingHandler();
    }

    protected int updateRow(TableMate table, Row row) {
        session.checkCanceled();
        RoutingResult result = routingHandler.doRoute(table, row);
        return invokeUpdateRow(result, row);
    }

    protected int updateRow(TableMate table, Row row, List<IndexCondition> where) {
        session.checkCanceled();
        RoutingResult result = routingHandler.doRoute(table, session, where);
        return invokeUpdateRow(result, row);
    }

    protected int updateRows(TableMate table, List<Row> rows) {
        Map<BatchKey, List<List<Value>>> batches = New.hashMap();
        session.checkCanceled();
        for (Row row : rows) {
            RoutingResult result = routingHandler.doRoute(table, row);
            TableNode[] selectNodes = result.getSelectNodes();
            for (TableNode node : selectNodes) {
                StatementBuilder sqlBuff = new StatementBuilder();
                List<Value> params = doTranslate(node, row, sqlBuff);
                BatchKey batchKey = new BatchKey(node.getShardName(), sqlBuff.toString());
                List<List<Value>> batchArgs = batches.get(batchKey);
                if (batchArgs == null) {
                    batchArgs = New.arrayList(10);
                    batches.put(batchKey, batchArgs);
                }
                batchArgs.add(params);
            }
        }
        List<JdbcWorker<Integer[]>> workers = New.arrayList(batches.size());
        for (Map.Entry<BatchKey, List<List<Value>>> entry : batches.entrySet()) {
            String shardName = entry.getKey().shardName;
            String sql = entry.getKey().sql;
            List<List<Value>> array = entry.getValue();
            workers.add(createBatchUpdateWorker(shardName, sql, array));
        }
        try {
            addRuningJdbcWorkers(workers);
            int affectRows = 0;
            if (workers.size() > 1) {
                int queryTimeout = getQueryTimeout();//MILLISECONDS
                List<Future<Integer[]>> invokeAll;
                if(queryTimeout > 0) {
                    invokeAll = jdbcExecutor.invokeAll(workers,queryTimeout,TimeUnit.MILLISECONDS);
                } else {
                    invokeAll = jdbcExecutor.invokeAll(workers);
                }
                for (Future<Integer[]> future : invokeAll) {
                    Integer[] integers = future.get();
                    for (Integer integer : integers) {
                        affectRows += integer;
                    }
                }
            } else if (workers.size() == 1) {
                Integer[] integers = workers.get(0).doWork();
                for (Integer integer : integers) {
                    affectRows += integer;
                }
            }
            return affectRows;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            removeRuningJdbcWorkers(workers);
            for (JdbcWorker<Integer[]> jdbcWorker : workers) {
                jdbcWorker.closeResource();
            }
        }
    }

    protected abstract List<Value> doTranslate(TableNode node, SearchRow row, StatementBuilder buff);

    protected static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    /**
     * @param result
     * @param row
     * @return
     */
    private int invokeUpdateRow(RoutingResult result, Row row) {
        List<JdbcWorker<Integer>> workers = New.arrayList(result.tableNodeCount());
        TableNode[] selectNodes = result.getSelectNodes();
        for (TableNode node : selectNodes) {
            StatementBuilder sqlBuff = new StatementBuilder();
            List<Value> params = doTranslate(node, row, sqlBuff);
            workers.add(createUpdateWorker(node.getShardName(), sqlBuff.toString(), params));
        }
        try {
            addRuningJdbcWorkers(workers);
            int affectRows = 0;
            if (workers.size() > 1) {
                int queryTimeout = getQueryTimeout();//MILLISECONDS
                List<Future<Integer>> invokeAll;
                if(queryTimeout > 0) {
                    invokeAll = jdbcExecutor.invokeAll(workers,queryTimeout,TimeUnit.MILLISECONDS);
                } else {
                    invokeAll = jdbcExecutor.invokeAll(workers);
                }
                for (Future<Integer> future : invokeAll) {
                    affectRows += future.get();
                }
            } else if (workers.size() == 1) {
                affectRows = workers.get(0).doWork();
            }
            return affectRows;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            removeRuningJdbcWorkers(workers);
            for (JdbcWorker<Integer> jdbcWorker : workers) {
                jdbcWorker.closeResource();
            }
        }
    }
    /**
     * build insert statement
     * @param forTable
     * @param columns
     * @param row
     * @param buff
     * @return
     */
    protected List<Value> buildInsert(String forTable, Column[] columns, SearchRow row, StatementBuilder buff) {
        ArrayList<Value> params = New.arrayList();
        buff.append("INSERT INTO ");
        buff.append(identifier(forTable)).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(") ");
        buff.resetCount();
        buff.append("VALUES( ");
        for (int i = 0; i < columns.length; i++) {
            Value v = row.getValue(i);
            buff.appendExceptFirst(", ");
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
            }
        }
        buff.append(")");
        return params;
    }

    private static class BatchKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String shardName;
        private final String sql;

        /**
         * @param shardName
         * @param sql
         */
        private BatchKey(String shardName, String sql) {
            super();
            this.shardName = shardName;
            this.sql = sql;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
            result = prime * result + ((sql == null) ? 0 : sql.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BatchKey other = (BatchKey) obj;
            if (shardName == null) {
                if (other.shardName != null)
                    return false;
            } else if (!shardName.equals(other.shardName))
                return false;
            if (sql == null) {
                if (other.sql != null)
                    return false;
            } else if (!sql.equals(other.sql))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "BatchKey [shardName=" + shardName + ", sql=" + sql + "]";
        }
    }

}
