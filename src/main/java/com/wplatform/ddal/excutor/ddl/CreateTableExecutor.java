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

package com.wplatform.ddal.excutor.ddl;

import java.util.ArrayList;
import java.util.Map;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.ddl.AlterTableAddConstraint;
import com.wplatform.ddal.command.ddl.CreateIndex;
import com.wplatform.ddal.command.ddl.CreateTable;
import com.wplatform.ddal.command.ddl.DefineCommand;
import com.wplatform.ddal.command.dml.Insert;
import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.schema.Sequence;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.value.DataType;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateTableExecutor extends DefineCommandExecutor<CreateTable> {

    /**
     * @param prepared
     */
    public CreateTableExecutor(CreateTable prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        if (tableMate == null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        if (!tableMate.isInited()) {
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
                if(command.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL) {
                    AlterTableAddConstraint stmt = (AlterTableAddConstraint)command;
                    String refTableName = stmt.getRefTableName();
                    TableMate refTable = getTableMate(refTableName);
                    if(refTable != null && refTable.getPartitionNode().length > 1) {
                        TableNode[] tableNodes = tableMate.getPartitionNode();
                        TableNode[] refTableNodes = refTable.getPartitionNode();
                        Map<TableNode, TableNode> symmetryRelation = getSymmetryRelation(tableNodes, refTableNodes);
                        if (symmetryRelation == null) {
                            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                                    "Create foreign key for table,the original table and the reference table should be symmetrical.");
                        }
                    }
                }
            }
            TableNode[] nodes = tableMate.getPartitionNode();
            execute(nodes);
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
    
    @Override
    protected String doTranslate(TableNode tableNode) {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (prepared.isTemporary()) {
            buff.append("TEMPORARY ");
        }
        buff.append("TABLE ");
        if (prepared.isIfNotExists()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(identifier(tableNode.getCompositeObjectName()));
        if (prepared.getComment() != null) {
            //buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(prepared.getComment()));
        }
        buff.append("(");
        for (Column column : prepared.getColumns()) {
            buff.appendExceptFirst(", ");
            buff.append(column.getCreateSQL());
        }
        for (DefineCommand command : prepared.getConstraintCommands()) {
            buff.appendExceptFirst(", ");
            int type = command.getType();
            switch (type) {
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint)command;
                buff.append(" CONSTRAINT PRIMARY KEY");
                if (stmt.isPrimaryKeyHash()) {
                    buff.append(" USING HASH");
                }
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint)command;
                buff.append(" CONSTRAINT UNIQUE KEY");
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint)command;
                String enclose = StringUtils.enclose(stmt.getCheckExpression().getSQL());
                buff.append(" CHECK").append(enclose);                
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint)command;
                String refTableName = stmt.getRefTableName();
                TableMate table = getTableMate(stmt.getTableName());
                TableMate refTable = getTableMate(refTableName);
                if(refTable != null) {
                    TableNode[] partitionNode = refTable.getPartitionNode();
                    if(partitionNode.length > 1) {
                        TableNode[] tableNodes = table.getPartitionNode();
                        TableNode[] refTableNodes = partitionNode;
                        Map<TableNode, TableNode> symmetryRelation = getSymmetryRelation(tableNodes, refTableNodes);
                        TableNode relation = symmetryRelation.get(tableNode);
                        if (relation == null) {
                            throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                                    "The original table and reference table should be symmetrical.");
                        }
                        refTableName = relation.getCompositeObjectName();
                    } else if(partitionNode.length == 1){
                        refTableName = partitionNode[0].getCompositeObjectName();
                    }
                }
                
                IndexColumn[] cols = stmt.getIndexColumns();
                IndexColumn[] refCols = stmt.getRefIndexColumns();
                buff.resetCount();
                buff.append(" CONSTRAINT FOREIGN KEY(");
                for (IndexColumn c : cols) {
                    buff.appendExceptFirst(", ");
                    buff.append(c.columnName);
                }
                buff.append(")");
                buff.append(" REFERENCES ");
                buff.append(identifier(refTableName)).append("(");
                buff.resetCount();
                for (IndexColumn r : refCols) {
                    buff.appendExceptFirst(", ");
                    buff.append(r.columnName);
                }
                buff.append(")");
                break;
            }
            case CommandInterface.CREATE_INDEX: {
                CreateIndex stmt = (CreateIndex)command;
                if(stmt.isSpatial()) {
                    buff.append(" SPATIAL INDEX");
                } else {
                    buff.append(" INDEX");
                    if (stmt.isHash()) {
                        buff.append(" USING HASH");
                    }
                }
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            default:
                throw DbException.throwInternalError("type=" + type);
            }
        }
        buff.append(")");
        if (prepared.getTableEngine() != null) {
            buff.append(" ENGINE = ");
            buff.append(prepared.getTableEngine());

        }
        ArrayList<String> tableEngineParams = prepared.getTableEngineParams();
        if (tableEngineParams != null && tableEngineParams.isEmpty()) {
            buff.append("WITH ");
            buff.resetCount();
            for (String parameter : tableEngineParams) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.quoteIdentifier(parameter));
            }
        }
        if(prepared.getCharset() != null) {
            buff.append(" DEFAULT CHARACTER SET = ");
            buff.append(prepared.getCharset());
        }
        return buff.toString();

    }

}
