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

import com.wplatform.ddal.command.ddl.AlterTableAddConstraint;
import com.wplatform.ddal.command.ddl.DropTable;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DropTableExecutor extends DefineCommandExecutor<DropTable> {

    public DropTableExecutor(DropTable prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        prepareDrop(prepared);
        executeDrop(prepared);
        return 0;
    }

    @Override
    protected String doTranslate(TableNode tableNode) {
        String forTable = tableNode.getCompositeObjectName();
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE");
        if(prepared.isIfExists()) {
            sql.append(" IF EXISTS");
        }
        sql.append(" ").append(identifier(forTable));
        if(prepared.getDropAction() == AlterTableAddConstraint.CASCADE) {
            sql.append(" CASCADE");
        }
        return sql.toString();
    }
    

    private void prepareDrop(DropTable next) {
        String tableName = next.getTableName();
        TableMate table = getTableMate(tableName);
        if (table == null) {
            if (!next.isIfExists()) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            }
        }
        session.getUser().checkRight(table, Right.ALL);
        next = prepared.getNext();
        if (next != null) {
            prepareDrop(next);
        }
    }

    private void executeDrop(DropTable next) {
        String tableName = next.getTableName();
        TableMate table = findTableMate(tableName);
        if (table != null) {
            TableNode[] nodes = table.getPartitionNode();
            execute(nodes);
            table.markDeleted();
        }
        next = prepared.getNext();
        if (next != null) {
            executeDrop(next);
        }
    }
    

}
