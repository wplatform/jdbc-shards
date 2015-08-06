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

import java.util.ArrayList;
import java.util.HashSet;

import com.wplatform.ddal.command.CommandInterface;
import com.wplatform.ddal.command.dml.Insert;
import com.wplatform.ddal.command.dml.Query;
import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.dbobject.DbObject;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.schema.Sequence;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.IndexColumn;
import com.wplatform.ddal.dbobject.table.Table;
import com.wplatform.ddal.dbobject.table.TableMate;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.DataType;

/**
 * This class represents the statement CREATE TABLE
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateTable extends SchemaCommand {

    private final CreateTableData data = new CreateTableData();
    private final ArrayList<DefineCommand> constraintCommands = New.arrayList();
    private IndexColumn[] pkColumns;
    private boolean ifNotExists;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private boolean sortedInsertMode;
    private String charset;

    public CreateTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }

    public void setTemporary(boolean temporary) {
        data.temporary = temporary;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }

    /**
     * Add a column to this table.
     *
     * @param column the column to add
     */
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    /**
     * Add a constraint statement to this statement. The primary key definition
     * is one possible constraint statement.
     *
     * @param command the statement to add
     */
    public void addConstraintCommand(DefineCommand command) {
        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        } else {
            AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            } else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        //Database db = session.getDatabase();
        TableMate tableOrView = finalTableMate(data.tableName);
        if (tableOrView != null && !tableOrView.canAccess()) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.tableName);
        }
        if (asQuery != null) {
            asQuery.prepare();
            if (data.columns.size() == 0) {
                generateColumnsFromQuery();
            } else if (data.columns.size() != asQuery.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        if (pkColumns != null) {
            for (Column c : data.columns) {
                for (IndexColumn idxCol : pkColumns) {
                    if (c.getName().equals(idxCol.columnName)) {
                        c.setNullable(false);
                    }
                }
            }
        }
        data.id = getObjectId();
        data.create = create;
        data.session = session;
        // boolean isSessionTemporary = data.temporary && !data.globalTemporary;
        // Table table = getSchema().createTable(data);
        ArrayList<Sequence> sequences = New.arrayList();
        for (Column c : data.columns) {
            if (c.isAutoIncrement()) {
                int objId = getObjectId();
                c.convertAutoIncrementToSequence(session, getSchema(), objId, data.temporary);
            }
            Sequence seq = c.getSequence();
            if (seq != null) {
                sequences.add(seq);
            }
        }
        // table.setComment(comment);
        System.out.println(getPlanSQL());

        try {
            for (Column c : data.columns) {
                c.prepareExpression(session);
            }
            /*for (Sequence sequence : sequences) { table.addSequence(sequence);
             * } */
            for (DefineCommand command : constraintCommands) {
                command.setTransactional(transactional);
                command.update();
            }
            if (asQuery != null) {
                Insert insert = new Insert(session);
                insert.setSortedInsertMode(sortedInsertMode);
                insert.setQuery(asQuery);
                // insert.setTable(table);
                insert.setInsertFromSelect(true);
                insert.prepare();
                insert.update();
            }
            HashSet<DbObject> set = New.hashSet();
            set.clear();
            // table.addDependencies(set);
            for (DbObject obj : set) {
                /*
                 * if (obj == table) { continue; } */
                if (obj.getType() == DbObject.TABLE_OR_VIEW) {
                    if (obj instanceof Table) {
                        //Table t = (Table) obj;
                        /* if (t.getId() > table.getId()) { throw
                         * DbException.get( ErrorCode.FEATURE_NOT_SUPPORTED_1,
                         * "Table depends on another table " +
                         * "with a higher ID: " + t +
                         * ", this is currently not supported, " +
                         * "as it would prevent the database from " +
                         * "being re-opened"); } */
                    }
                }
            }
        } catch (DbException e) {
            //db.removeSchemaObject(session, table);
            throw e;
        }
        throw DbException.getUnsupportedException("TODO");
    }

    /**
     * Sets the primary key columns, but also check if a primary key with
     * different columns is already defined.
     *
     * @param columns the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(IndexColumn[] columns) {
        if (pkColumns != null) {
            int len = columns.length;
            if (len != pkColumns.length) {
                throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
            }
            for (int i = 0; i < len; i++) {
                if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
                    throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                }
            }
            return true;
        }
        this.pkColumns = columns;
        return false;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    public void setTableEngine(String tableEngine) {
        data.tableEngine = tableEngine;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        data.tableEngineParams = tableEngineParams;
    }

    public void setHidden(boolean isHidden) {
        data.isHidden = isHidden;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TABLE;
    }

    private void generateColumnsFromQuery() {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0
                    && (dt.defaultPrecision == 0 || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0
                    && (dt.defaultScale == 0 || (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            addColumn(col);
        }
    }

    @Override
    public String getPlanSQL() {
        return null;
    }

}
