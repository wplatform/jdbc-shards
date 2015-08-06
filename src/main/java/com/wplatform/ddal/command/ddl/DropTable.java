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
package com.wplatform.ddal.command.ddl;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Database;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;

/**
 * This class represents the statement
 * DROP TABLE
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DropTable extends SchemaCommand {

    private boolean ifExists;
    private String tableName;
    private Table table;
    private DropTable next;
    private int dropAction;

    public DropTable(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                Constants.RESTRICT :
                Constants.CASCADE;
    }

    /**
     * Chain another drop table statement to this statement.
     *
     * @param drop the statement to add
     */
    public void addNextDropTable(DropTable drop) {
        if (next == null) {
            next = drop;
        } else {
            next.addNextDropTable(drop);
        }
    }

    public void setIfExists(boolean b) {
        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public int update() {
        session.commit(true);
        prepareDrop();
        executeDrop();
        return 0;
    }

    public void setDropAction(int dropAction) {
        this.dropAction = dropAction;
        if (next != null) {
            next.setDropAction(dropAction);
        }
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TABLE;
    }


    private void prepareDrop() {
        table = finalTableMate(tableName);
        if (table == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            }
        }
        session.getUser().checkRight(table, Right.ALL);
        if (dropAction == Constants.CASCADE) {

        }
        if (next != null) {
            next.prepareDrop();
        }
    }

    private void executeDrop() {
        table = finalTableMate(tableName);
        if (table != null) {
            Database db = session.getDatabase();
            db.removeSchemaObject(session, table);
        }
        if (next != null) {
            next.executeDrop();
        }
    }


}