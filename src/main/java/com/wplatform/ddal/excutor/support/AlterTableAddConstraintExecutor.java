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
import java.util.Map;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.ddl.AlterTableAddConstraint;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.dbobject.index.IndexType;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class AlterTableAddConstraintExecutor extends CommonPreparedExecutor<AlterTableAddConstraint> {

    /**
     * @param session
     * @param prepared
     */
    public AlterTableAddConstraintExecutor(Session session, AlterTableAddConstraint prepared) {
        super(session, prepared);
    }

    @Override
    public int executeUpdate() {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        session.getUser().checkRight(table, Right.ALL);
        TableNode[] tableNodes = table.getPartitionNode();
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {

        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {

        }
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
            IndexColumn.mapColumns(indexColumns, table);
            
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + type);
    }
        
        return 0;
    }

    private static Map<TableNode, TableNode> getSymmetryRelation(TableNode[] n1, TableNode[] n2) {
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
    
    
    private String doTranslate(TableNode tableNode) {
        
        return null;
    }

}
