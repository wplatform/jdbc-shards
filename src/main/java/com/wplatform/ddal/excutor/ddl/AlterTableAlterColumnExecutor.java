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

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.ddl.AlterTableAlterColumn;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.schema.Sequence;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.CommonPreparedExecutor;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.StatementBuilder;
import com.wplatform.ddal.util.StringUtils;

/**
 * This executor execute the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class AlterTableAlterColumnExecutor extends CommonPreparedExecutor<AlterTableAlterColumn> {

    /**
     * @param session
     * @param prepared
     */
    public AlterTableAlterColumnExecutor(Session session, AlterTableAlterColumn prepared) {
        super(session, prepared);
    }

    @Override
    public int executeUpdate() {
        Table parseTable = prepared.getTable();
        if(!(parseTable instanceof TableMate)) {
            DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,parseTable.getSQL());
        }
        TableMate table = (TableMate)parseTable;
        session.getUser().checkRight(table, Right.ALL);
        TableNode[] tableNodes = table.getPartitionNode();
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: 
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT: 
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: 
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: 
        case CommandInterface.ALTER_TABLE_DROP_COLUMN:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
            executeOn(tableNodes);
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    
    @Override
    protected String doTranslate(TableNode node) {
        
        TableMate table = (TableMate)prepared.getTable();
        String forTable = node.getCompositeTableName();
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
            StatementBuilder buff = new StatementBuilder("ALTER TABLE");
            buff.append(forTable).append(" ADD CONSTRAINT ");
            Column oldColumn = prepared.getOldColumn();
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: {
            if (oldColumn.isNullable()) {
                // no change
                break;
            }
            checkNullable();
            oldColumn.setNullable(true);
            db.updateMeta(session, table);
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT: {
            StatementBuilder buff = new StatementBuilder("ALTER TABLE");
            buff.append(" ALTER COLUMN ");
            buff.append(prepared.getOldColumn().getSQL());
            buff.append(" SET DEFAULT ");
            buff.append(prepared.getDefaultExpression().getSQL());
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
            // if the change is only increasing the precision, then we don't
            // need to copy the table because the length is only a constraint,
            // and does not affect the storage structure.
            StatementBuilder buff = new StatementBuilder("ALTER TABLE");
            buff.append(" ALTER COLUMN ");
            buff.append(prepared.getOldColumn().getSQL());
            buff.append(" SET DEFAULT ");
            buff.append(prepared.getDefaultExpression().getSQL());
            
            if (prepared.getOldColumn().isWideningConversion(newColumn)) {
                convertAutoIncrementColumn(newColumn);
                oldColumn.copy(newColumn);
                db.updateMeta(session, table);
            } else {
                oldColumn.setSequence(null);
                oldColumn.setDefaultExpression(session, null);
                oldColumn.setConvertNullToDefault(false);
                if (oldColumn.isNullable() && !newColumn.isNullable()) {
                    checkNoNullValues();
                } else if (!oldColumn.isNullable() && newColumn.isNullable()) {
                    checkNullable();
                }
                convertAutoIncrementColumn(newColumn);
                copyData();
            }
            break;
        }
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
            // ifNotExists only supported for single column add
            if (ifNotExists && columnsToAdd.size() == 1 &&
                    table.doesColumnExist(columnsToAdd.get(0).getName())) {
                break;
            }
            for (Column column : columnsToAdd) {
                if (column.isAutoIncrement()) {
                    int objId = getObjectId();
                    column.convertAutoIncrementToSequence(session, getSchema(), objId,
                            table.isTemporary());
                }
            }
            copyData();
            break;
        }
        case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
            if (table.getColumns().length - columnsToRemove.size() < 1) {
                throw DbException.get(ErrorCode.CANNOT_DROP_LAST_COLUMN,
                        columnsToRemove.get(0).getSQL());
            }
            table.dropMultipleColumnsConstraintsAndIndexes(session, columnsToRemove);
            copyData();
            break;
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
            int value = newSelectivity.optimize(session).getValue(session).getInt();
            oldColumn.setSelectivity(value);
            db.updateMeta(session, table);
            break;
        }
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    
        
    }
    

}
