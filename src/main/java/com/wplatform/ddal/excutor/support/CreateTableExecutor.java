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

package com.wplatform.ddal.excutor.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.wplatform.ddal.command.ddl.CreateTable;
import com.wplatform.ddal.command.ddl.DefineCommand;
import com.wplatform.ddal.command.dml.Insert;
import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.Parameter;
import com.wplatform.ddal.dbobject.schema.Sequence;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.excutor.UpdateResult;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.value.DataType;
import com.wplatform.ddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateTableExecutor extends CommonPreparedExecutor<CreateTable> {

    /**
     * @param session
     * @param prepared
     */
    public CreateTableExecutor(Session session, CreateTable prepared) {
        super(session, prepared);
    }

    @Override
    public int executeUpdate() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        if (tableMate == null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        if (!tableMate.isMock()) {
            if (prepared.isIfNotExists()) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
        }
        Query query = prepared.getQuery();
        if (query != null) {
            query.prepare();
            if (prepared.getColumnCount() == 0) {
                generateColumnsFromQuery();
            } else if (prepared.getColumnCount() != query.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }

        ArrayList<Sequence> sequences = New.arrayList();
        for (Column c : tableMate.getColumns()) {
            if (c.isAutoIncrement()) {
                c.convertAutoIncrementToSequence(session, database.getSchema(session.getCurrentSchemaName()),
                        tableMate.getId(), prepared.isTemporary());
            }
            Sequence seq = c.getSequence();
            if (seq != null) {
                sequences.add(seq);
            }
        }

        try {
            for (Column c : tableMate.getColumns()) {
                c.prepareExpression(session);
            }
            for (Sequence sequence : sequences) {
                tableMate.addSequence(sequence);
            }
            for (DefineCommand command : prepared.getConstraintCommands()) {
                command.setTransactional(prepared.isTransactional());
                command.update();
            }
            TableNode[] nodes = tableMate.getPartitionNode();
            List<Worker<UpdateResult>> workers = New.arrayList(nodes.length);
            for (TableNode node : nodes) {
                String sql = doTranslate(node);
                List<Parameter> items = getPrepared().getParameters();
                List<Value> params = New.arrayList(items.size());
                for (Parameter parameter : items) {
                    params.add(parameter.getParamValue());
                }
                workers.add(createUpdateWorker(node.getShardName(), sql, params));
            }
            try {
                List<Future<UpdateResult>> futures = sqlExecutor.invokeAll(workers);
                for (Future<UpdateResult> future : futures) {
                    
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            if (query != null) {
                Insert insert = new Insert(session);
                insert.setSortedInsertMode(prepared.isSortedInsertMode());
                insert.setQuery(query);
                insert.setTable(tableMate);
                insert.setInsertFromSelect(true);
                insert.prepare();
                insert.update();
            }
        } catch (DbException e) {
            throw e;
        }
        tableMate.loadMataData(session);
        return 0;
    }

    private void generateColumnsFromQuery() {
        int columnCount = prepared.getQuery().getColumnCount();
        ArrayList<Expression> expressions = prepared.getQuery().getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0
                    || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 || (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            prepared.addColumn(col);
        }
    }

    private String doTranslate(TableNode tableNode) {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (prepared.isTemporary()) {
            buff.append("TEMPORARY ");
        }
        buff.append("TABLE ");
        if (prepared.isIfNotExists()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(tableNode.getCompositeTableName());
        if (prepared.getComment() != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(prepared.getComment()));
        }
        buff.append("(\n    ");
        for (Column column : prepared.getColumns()) {
            buff.appendExceptFirst(",\n    ");
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        if (prepared.getTableEngine() != null) {
            buff.append("\nENGINE ");
            buff.append(StringUtils.quoteIdentifier(prepared.getTableEngine()));

        }
        if (!prepared.getTableEngineParams().isEmpty()) {
            buff.append("\nWITH ");
            buff.resetCount();
            for (String parameter : prepared.getTableEngineParams()) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.quoteIdentifier(parameter));
            }
        }
        return buff.toString();

    }

}
