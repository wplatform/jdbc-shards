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
package com.wplatform.ddal.excutor.ddl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.wplatform.ddal.command.ddl.DefineCommand;
import com.wplatform.ddal.command.expression.Parameter;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.excutor.JdbcWorker;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class DefineCommandExecutor<T extends DefineCommand> extends CommonPreparedExecutor<T> {

    public DefineCommandExecutor(T prepared) {
        super(prepared);
    }

    /**
     * execute DDL use default sql translator
     * 
     * @param nodes
     */
    public void execute(TableNode[] nodes) {
        session.checkCanceled();
        List<JdbcWorker<Integer>> workers = New.arrayList(nodes.length);
        for (TableNode node : nodes) {
            String sql = doTranslate(node);
            List<Parameter> items = getPrepared().getParameters();
            List<Value> params = New.arrayList(items.size());
            for (Parameter parameter : items) {
                params.add(parameter.getParamValue());
            }
            workers.add(createUpdateWorker(node.getShardName(), sql, params));
        }
        addRuningJdbcWorkers(workers);
        try {
            // DDL statement returns nothing.
            if (workers.size() > 1) {
                int queryTimeout = getQueryTimeout();//MILLISECONDS
                if(queryTimeout > 0) {
                    jdbcExecutor.invokeAll(workers,queryTimeout,TimeUnit.MILLISECONDS);
                } else {
                    jdbcExecutor.invokeAll(workers);
                }    
            } else if (workers.size() == 1) {
                workers.get(0).doWork();
            }
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } finally {
            removeRuningJdbcWorkers(workers);
            for (JdbcWorker<Integer> jdbcWorker : workers) {
                jdbcWorker.closeResource();
            }
        }
    }
    
    protected abstract String doTranslate(TableNode node);

    
    protected static Map<TableNode, TableNode> getSymmetryRelation(TableNode[] n1, TableNode[] n2) {
        if (n1.length != n2.length) {
            return null;
        }
        Map<TableNode, TableNode> tableNode = New.hashMap();
        for (TableNode tn1 : n1) {
            String sName = tn1.getShardName();
            String suffix = tn1.getSuffix();
            TableNode matched = null;
            for (TableNode tn2 : n2) {
                if (!sName.equals(tn2.getShardName())) {
                    continue;
                }
                if (suffix != null && !suffix.equals(tn2.getSuffix())) {
                    continue;
                }
                matched = tn2;
            }
            if (matched == null) {
                return null;
            }
            tableNode.put(tn1, matched);
        }
        if (tableNode.size() != n1.length) {
            return null;
        }
        return tableNode;
    }


}
