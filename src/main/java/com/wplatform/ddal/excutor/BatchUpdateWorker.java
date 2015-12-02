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
package com.wplatform.ddal.excutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class BatchUpdateWorker extends JdbcWorker<Integer[]> {

    protected final List<List<Value>> array;

    public BatchUpdateWorker(Session session, String shardName, String sql, List<List<Value>> array) {
        super(session, shardName, sql, null);
        this.array = array;
    }
    
    
    @Override
    public Integer[] doWork() {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            if (array == null || array.size() < 1) {
                throw new IllegalArgumentException();
            }
            DataSource dataSource = getDataSource();
            Optional optional = Optional.build().shardName(shardName).readOnly(false);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            conn = session.applyConnection(dataSource, optional);
            attach(conn);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {};", shardName, sql);
            }
            stmt = conn.prepareStatement(sql);
            attach(stmt);
            applyQueryTimeout(stmt);
            for (List<Value> params : array) {
                if (params != null) {
                    for (int i = 0, size = params.size(); i < size; i++) {
                        Value v = params.get(i);
                        v.set(stmt, i + 1);
                        if (trace.isDebugEnabled()) {
                            trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                        }
                    }
                    stmt.addBatch();
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} addBatch.", shardName);
                    }
                }
            }
            int[] affected = stmt.executeBatch();
            Integer[] rows = new Integer[affected.length];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = affected[i];
            }
            if (trace.isDebugEnabled()) {
                trace.debug("{0} executeUpdate: {1} affected.", shardName, Arrays.toString(affected));
            }
            return rows;
        } catch (SQLException e) {
            error(e);
            throw wrapException(sql, e);
        } catch (Throwable e) {
            error(e);
            throw DbException.convert(e);
        }
    }


    /**
     * @param e
     */
    protected void error(Throwable e) {
        StatementBuilder buff = new StatementBuilder();
        buff.append(shardName).append(" executing batchUpdate error:").append(sql);
        for (List<Value> params : array) {
            if (params != null) {
                if (params != null && params.size() > 0) {
                    buff.appendExceptFirst(", ");
                    buff.append("\n{");
                    int i = 1;
                    for (Value v : params) {
                        buff.appendExceptFirst(", ");
                        buff.append(i++).append(": ").append(v.getSQL());
                    }
                    buff.append('}');
                }
            }
        }
        buff.append(';');
        trace.error(e, buff.toString());
    }

}
