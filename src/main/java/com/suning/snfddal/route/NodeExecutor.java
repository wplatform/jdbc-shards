/*
 * Copyright 2015 suning.com Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2015年1月14日
// $Id$

package com.suning.snfddal.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import java.sql.Statement;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.Trace;
import com.suning.snfddal.util.JdbcUtils;
import com.suning.snfddal.util.StatementBuilder;
import com.suning.snfddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class NodeExecutor {
    
    private final Session session;
    private final Trace trace;
    public NodeExecutor(Session session) {
        this.session = session;
        this.trace = session.getTrace();
    }
    
    
    public int executeUpdate(NodeExecution execution) {
        Statement stat = preparedAndExecute(execution);
        try {
            return stat.getUpdateCount();
        } catch(SQLException e) {
            throw DbException.convert(e);
        }
    }

    public ResultSet executeQuery(NodeExecution execution) {
        Statement stmt = preparedAndExecute(execution);
        try {
            return stmt.getResultSet();
        } catch(SQLException e) {
            throw DbException.convert(e);
        }
    }
    
    private PreparedStatement preparedAndExecute(NodeExecution execution) {
        if(execution.isBatch()) {
            DbException.throwInternalError("Illegal argement.");
        }
        String shardName = execution.getShardName();
        String sql = execution.getSql();
        List<Value> params = execution.getParams();
        
        
        Connection conn = null;
        PreparedStatement prep = null;
        try {
            conn = session.getDataNodeConnection(shardName);
            prep = conn.prepareStatement(sql);
            if (trace.isDebugEnabled()) {
                StatementBuilder buff = new StatementBuilder();
                buff.append("executing ").append(sql);
                if (params != null && params.size() > 0) {
                    buff.append(" params:{");
                    int i = 1;
                    for (Value v : params) {
                        buff.appendExceptFirst(", ");
                        buff.append(i++).append(": ").append(v.getSQL());
                    }
                    buff.append('}');
                }
                buff.append(';');
                trace.debug(buff.toString());
            }
            if (params != null) {
                for (int i = 0, size = params.size(); i < size; i++) {
                    Value v = params.get(i);
                    v.set(prep, i + 1);
                }
            }
            prep.execute();
            return prep;
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(prep);
            // JdbcUtils.closeSilently(conn)
        }
    }
}
