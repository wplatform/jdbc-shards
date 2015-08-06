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

import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;

/**
 * This class represents a non-transaction statement that involves a schema.
 */
public abstract class SchemaCommand extends DefineCommand {

    private final Schema schema;

    /**
     * Create a new command.
     *
     * @param session the session
     * @param schema  the schema
     */
    public SchemaCommand(Session session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    /**
     * Get the schema
     *
     * @return the schema
     */
    protected Schema getSchema() {
        return schema;
    }


    /**
     * @param tableName
     */
    public TableMate finalTableMate(String tableName) {
        Table tableOrView = getSchema().findTableOrView(session, tableName);
        TableMate tableMate = null;
        if (tableOrView != null && tableOrView instanceof TableMate) {
            tableMate = (TableMate) tableOrView;
        }
        return tableMate;
    }


    /**
     * @param tableOrView
     */
    public TableMate getTableMate(String tableName) {
        Table tableOrView = getSchema().getTableOrView(session, tableName);
        if (tableOrView != null && tableOrView instanceof TableMate) {
            return (TableMate) tableOrView;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

}
