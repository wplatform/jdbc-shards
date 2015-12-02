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

import java.util.Map;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.ddl.AlterTableAddConstraint;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class AlterTableAddConstraintExecutor extends DefineCommandExecutor<AlterTableAddConstraint> {

    /**
     * @param session
     * @param prepared
     */
    public AlterTableAddConstraintExecutor(AlterTableAddConstraint prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        session.getUser().checkRight(table, Right.ALL);
        TableNode[] tableNodes = table.getPartitionNode();
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
            String refTableName = prepared.getRefTableName();
            TableMate refTable = getTableMate(refTableName);
            TableNode[] refTableNode = table.getPartitionNode();
            Map<TableNode, TableNode> symmetryRelation = getSymmetryRelation(tableNodes, refTableNode);
            if (symmetryRelation == null) {
                throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                        "The original table and reference table should be symmetrical.");
            }
            session.getUser().checkRight(refTable, Right.ALL);
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
            execute(tableNodes);
            break;
        }

        default:
            throw DbException.throwInternalError("type=" + type);
        }

        return 0;
    }

    @Override
    protected String doTranslate(TableNode tableNode) {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        String forTable = tableNode.getCompositeObjectName();
        IndexColumn.mapColumns(prepared.getIndexColumns(), table);
        switch (prepared.getType()) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
            return doBuildUnique(forTable, AlterTableAddConstraint.PRIMARY_KEY);
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {
            String uniqueType = AlterTableAddConstraint.UNIQUE + " KEY";
            return doBuildUnique(forTable, uniqueType);
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
            /*
             * MySQL. The CHECK clause is parsed but ignored by all storage
             * engines.
             */
            return doBuildCheck(forTable);
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
            /*
             * MySQL. The FOREIGN KEY and REFERENCES clauses are supported by
             * the InnoDB and NDB storage engines
             */
            String refTableName = prepared.getRefTableName();
            TableMate refTable = getTableMate(refTableName);
            Map<TableNode, TableNode> symmetryRelation = getSymmetryRelation(table.getPartitionNode(),
                    refTable.getPartitionNode());
            TableNode relation = symmetryRelation.get(tableNode);
            if (relation == null) {
                throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                        "The original table and reference table should be symmetrical.");
            }
            return doBuildReferences(forTable, relation.getCompositeObjectName());
        }
        default:
            throw DbException.throwInternalError("type=" + prepared.getType());

        }
    }

    private String doBuildUnique(String forTable, String uniqueType) {
        StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
        buff.append(identifier(forTable)).append(" ADD CONSTRAINT ");
        String constraintName = prepared.getConstraintName();
        // MySQL constraintName is optional
        if (!StringUtils.isNullOrEmpty(constraintName)) {
            buff.append(constraintName);
        }
        buff.append(' ').append(uniqueType);
        if (prepared.isPrimaryKeyHash()) {
            buff.append(" USING ").append("HASH");
        }
        buff.append('(');
        for (IndexColumn c : prepared.getIndexColumns()) {
            buff.appendExceptFirst(", ");
            buff.append(identifier(c.column.getName()));
        }
        buff.append(')');
        return buff.toString();
    }

    private String doBuildCheck(String forTable) {
        StringBuilder buff = new StringBuilder("ALTER TABLE ");
        buff.append(identifier(forTable)).append(" ADD CONSTRAINT ");
        String constraintName = prepared.getConstraintName();
        if (!StringUtils.isNullOrEmpty(constraintName)) {
            buff.append(constraintName);
        }
        String enclose = StringUtils.enclose(prepared.getCheckExpression().getSQL());
        buff.append(" CHECK").append(enclose).append(" NOCHECK");
        return buff.toString();
    }

    private String doBuildReferences(String forTable, String forRefTable) {
        StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
        buff.append(identifier(forTable)).append(" ADD CONSTRAINT ");
        String constraintName = prepared.getConstraintName();
        if (!StringUtils.isNullOrEmpty(constraintName)) {
            buff.append(constraintName);
        }
        IndexColumn[] cols = prepared.getIndexColumns();
        IndexColumn[] refCols = prepared.getRefIndexColumns();
        buff.append(" FOREIGN KEY(");
        for (IndexColumn c : cols) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');

        buff.append(" REFERENCES ");
        buff.append(forRefTable).append('(');
        buff.resetCount();
        for (IndexColumn r : refCols) {
            buff.appendExceptFirst(", ");
            buff.append(r.getSQL());
        }
        buff.append(')');
        if (prepared.getDeleteAction() != AlterTableAddConstraint.RESTRICT) {
            buff.append(" ON DELETE ");
            appendAction(buff, prepared.getDeleteAction());
        }
        if (prepared.getUpdateAction() != AlterTableAddConstraint.RESTRICT) {
            buff.append(" ON UPDATE ");
            appendAction(buff, prepared.getDeleteAction());
        }
        return buff.toString();
    }

    private static void appendAction(StatementBuilder buff, int action) {
        switch (action) {
        case AlterTableAddConstraint.CASCADE:
            buff.append("CASCADE");
            break;
        case AlterTableAddConstraint.SET_DEFAULT:
            buff.append("SET DEFAULT");
            break;
        case AlterTableAddConstraint.SET_NULL:
            buff.append("SET NULL");
            break;
        default:
            DbException.throwInternalError("action=" + action);
        }
    }

}
