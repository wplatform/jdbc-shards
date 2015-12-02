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
// Created on 2014年12月25日
// $Id$
package com.wplatform.ddal.dbobject.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.wplatform.ddal.command.expression.Expression;
import com.wplatform.ddal.command.expression.ExpressionVisitor;
import com.wplatform.ddal.dbobject.DbObject;
import com.wplatform.ddal.dbobject.Right;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dbobject.schema.SchemaObjectBase;
import com.wplatform.ddal.dbobject.schema.Sequence;
import com.wplatform.ddal.engine.Constants;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.message.Trace;
import com.wplatform.ddal.result.Row;
import com.wplatform.ddal.result.SearchRow;
import com.wplatform.ddal.result.SimpleRow;
import com.wplatform.ddal.result.SimpleRowValue;
import com.wplatform.ddal.result.SortOrder;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.value.Value;
import com.wplatform.ddal.value.ValueNull;

/**
 * This is the base class for most tables. A table contains a list of columns
 * and a list of rows.
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class Table extends SchemaObjectBase {

    /**
     * The table type name for regular data tables.
     */
    public static final String TABLE = "TABLE";

    /**
     * The table type name for views.
     */
    public static final String VIEW = "VIEW";

    /**
     * The columns of this table.
     */
    protected Column[] columns;

    private final HashMap<String, Column> columnMap;
    private ArrayList<Sequence> sequences;
    private Row nullRow;

    public Table(Schema schema, int id, String name) {
        columnMap = schema.getDatabase().newStringMap();
        initSchemaObjectBase(schema, id, name, Trace.TABLE);
        // compareMode = schema.getDatabase().getCompareMode();
    }

    @Override
    public void rename(String newName) {
        super.rename(newName);
    }

    /**
     * Get the table type name
     *
     * @return the table type name
     */
    public abstract String getTableType();

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(Session session);

    /**
     * Get any unique index for this table if one exists.
     *
     * @return a unique index
     */
    public abstract Index getUniqueIndex();

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ArrayList<Index> getIndexes();

    /**
     * Check if the table is deterministic.
     *
     * @return true if it is
     */
    public abstract boolean isDeterministic();

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @return true if it can
     */
    public abstract boolean canGetRowCount();

    /**
     * Get the row count for this table.
     *
     * @param session the session
     * @return the row count
     */
    public abstract long getRowCount(Session session);

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation();

    /**
     * Get the row id column if this table has one.
     *
     * @return the row id column, or null
     */
    public Column getRowIdColumn() {
        return null;
    }

    /**
     * Check whether the table (or view) contains no columns that prevent index
     * conditions to be used. For example, a view that contains the ROWNUM()
     * pseudo-column prevents this.
     *
     * @return true if the table contains no query-comparable column
     */
    public boolean isQueryComparable() {
        return true;
    }

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public void addDependencies(HashSet<DbObject> dependencies) {
        if (dependencies.contains(this)) {
            // avoid endless recursion
            return;
        }
        if (sequences != null) {
            for (Sequence s : sequences) {
                dependencies.add(s);
            }
        }
        ExpressionVisitor visitor = ExpressionVisitor
                .getDependenciesVisitor(dependencies);
        for (Column col : columns) {
            col.isEverything(visitor);
        }
        dependencies.add(this);
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        ArrayList<DbObject> children = New.arrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            children.addAll(indexes);
        }
        if (sequences != null) {
            children.addAll(sequences);
        }
        ArrayList<Right> rights = database.getAllRights();
        for (Right right : rights) {
            if (right.getGrantedTable() == this) {
                children.add(right);
            }
        }
        return children;
    }

    protected void setColumns(Column[] columns) {
        this.columns = columns;
        if (columnMap.size() > 0) {
            columnMap.clear();
        }
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            int dataType = col.getType();
            if (dataType == Value.UNKNOWN) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1,
                        col.getSQL());
            }
            col.setTable(this, i);
            String columnName = col.getName();
            if (columnMap.get(columnName) != null) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1,
                        columnName);
            }
            columnMap.put(columnName, col);
        }
    }

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public void renameColumn(Column column, String newName) {
        for (Column c : columns) {
            if (c == column) {
                continue;
            }
            if (c.getName().equals(newName)) {
                throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1,
                        newName);
            }
        }
        columnMap.remove(column.getName());
        column.rename(newName);
        columnMap.put(newName, column);
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        for (Right right : database.getAllRights()) {
            if (right.getGrantedTable() == this) {
                database.removeDatabaseObject(session, right);
            }
        }
        // must delete sequences later (in case there is a power failure
        // before removing the table object)
        while (sequences != null && sequences.size() > 0) {
            Sequence sequence = sequences.get(0);
            sequences.remove(0);
            if (!isTemporary()) {
                // only remove if no other table depends on this sequence
                // this is possible when calling ALTER TABLE ALTER COLUMN
                if (database.getDependentTable(sequence, this) == null) {
                    database.removeSchemaObject(session, sequence);
                }
            }
        }
    }

    /**
     * Check that this column is not referenced by a multi-column constraint or
     * multi-column index. If it is, an exception is thrown. Single-column
     * references and indexes are dropped.
     *
     * @param session the session
     * @param col the column
     * @throws DbException if the column is referenced by multi-column
     *             constraints or indexes
     */
    public void dropSingleColumnConstraintsAndIndexes(Session session,
            Column col) {
        ArrayList<Index> indexesToDrop = New.arrayList();
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                if (index.getColumnIndex(col) < 0) {
                    continue;
                }
                if (index.getColumns().length == 1) {
                    indexesToDrop.add(index);
                } else {
                    throw DbException.get(ErrorCode.COLUMN_IS_REFERENCED_1,
                            index.getSQL());
                }
            }
        }
        for (Index i : indexesToDrop) {
            // the index may already have been dropped when dropping the
            // constraint
            if (getIndexes().contains(i)) {
                session.getDatabase().removeSchemaObject(session, i);
            }
        }
    }

    public Row getTemplateRow() {
        return new Row(new Value[columns.length], Row.MEMORY_CALCULATE);
    }

    /**
     * Get a new simple row object.
     *
     * @param singleColumn if only one value need to be stored
     * @return the simple row object
     */
    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        if (singleColumn) {
            return new SimpleRowValue(columns.length);
        }
        return new SimpleRow(new Value[columns.length]);
    }

    synchronized Row getNullRow() {
        if (nullRow == null) {
            nullRow = new Row(new Value[columns.length], 1);
            for (int i = 0; i < columns.length; i++) {
                nullRow.setValue(i, ValueNull.INSTANCE);
            }
        }
        return nullRow;
    }

    public Column[] getColumns() {
        return columns;
    }

    @Override
    public int getType() {
        return DbObject.TABLE_OR_VIEW;
    }

    /**
     * Get the column at the given index.
     *
     * @param index the column index (0, 1,...)
     * @return the column
     */
    public Column getColumn(int index) {
        return columns[index];
    }

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @return the column
     * @throws DbException if the column was not found
     */
    public Column getColumn(String columnName) {
        Column column = columnMap.get(columnName);
        if (column == null) {
            throw DbException.get(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
        }
        return column;
    }

    /**
     * Does the column with the given name exist?
     *
     * @param columnName the column name
     * @return true if the column exists
     */
    public boolean doesColumnExist(String columnName) {
        return columnMap.containsKey(columnName);
    }

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *            see constants in IndexCondition
     * @param filter the table filter
     * @param sortOrder the sort order
     * @return the plan item
     */
    public PlanItem getBestPlanItem(Session session, int[] masks,
            TableFilter filter, SortOrder sortOrder) {
        PlanItem item = new PlanItem();
        item.setIndex(getScanIndex(session));
        item.cost = item.getIndex().getCost(session, null, null, null);
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null && masks != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                double cost = index.getCost(session, masks, filter, sortOrder);
                if (cost < item.cost) {
                    item.cost = cost;
                    item.setIndex(index);
                }
            }
        }
        return item;
    }

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public Index findPrimaryKey() {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 0, size = indexes.size(); i < size; i++) {
                Index idx = indexes.get(i);
                if (idx.getIndexType().isPrimaryKey()) {
                    return idx;
                }
            }
        }
        return null;
    }

    public Index getPrimaryKey() {
        Index index = findPrimaryKey();
        if (index != null) {
            return index;
        }
        throw DbException.get(ErrorCode.INDEX_NOT_FOUND_1,
                Constants.PREFIX_PRIMARY_KEY);
    }

    /**
     * Validate all values in this row, convert the values if required, and
     * update the sequence values if required. This call will also set the
     * default values if required and set the computed column if there are any.
     *
     * @param session the session
     * @param row the row
     */
    public void validateConvertUpdateSequence(Session session, Row row) {
        for (int i = 0; i < columns.length; i++) {
            Value value = row.getValue(i);
            Column column = columns[i];
            Value v2;
            if (column.getComputed()) {
                // force updating the value
                value = null;
                v2 = column.computeValue(session, row);
            }
            v2 = column.validateConvertUpdateSequence(session, value);
            if (v2 != value) {
                row.setValue(i, v2);
            }
        }
    }

    private static void remove(ArrayList<? extends DbObject> list, DbObject obj) {
        if (list != null) {
            int i = list.indexOf(obj);
            if (i >= 0) {
                list.remove(i);
            }
        }
    }

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public void removeIndex(Index index) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            remove(indexes, index);
            if (index.getIndexType().isPrimaryKey()) {
                for (Column col : index.getColumns()) {
                    col.setPrimaryKey(false);
                }
            }
        }
    }

    /**
     * Remove a sequence from the table. Sequences are used as identity columns.
     *
     * @param sequence the sequence to remove
     */
    public final void removeSequence(Sequence sequence) {
        remove(sequences, sequence);
    }

    /**
     * Add a sequence to this table.
     *
     * @param sequence the sequence to add
     */
    public void addSequence(Sequence sequence) {
        sequences = add(sequences, sequence);
    }

    private static <T> ArrayList<T> add(ArrayList<T> list, T obj) {
        if (list == null) {
            list = New.arrayList();
        }
        // self constraints are two entries in the list
        list.add(obj);
        return list;
    }

    public boolean isGlobalTemporary() {
        return false;
    }

    /**
     * Get the index that has the given column as the first element. This method
     * returns null if no matching index is found.
     *
     * @param column the column
     * @return the index or null
     */
    public Index getIndexForColumn(Column column) {
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null) {
            for (int i = 1, size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                int idx = index.getColumnIndex(column);
                if (idx == 0) {
                    return index;
                }

            }
        }
        return null;
    }

    /**
     * Get or generate a default value for the given column.
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    public Value getDefaultValue(Session session, Column column) {
        Expression defaultExpr = column.getDefaultExpression();
        Value v;
        if (defaultExpr == null) {
            v = column.validateConvertUpdateSequence(session, null);
        } else {
            v = defaultExpr.getValue(session);
        }
        return column.convert(v);
    }

}
