/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.command.Parser;
import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.Right;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.index.IndexType;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.Column;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.dbobject.table.Table;
import com.suning.snfddal.dbobject.table.TableMate;
import com.suning.snfddal.dispatch.rule.TableNode;
import com.suning.snfddal.engine.Database;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.message.ErrorCode;
import com.suning.snfddal.util.New;
import com.suning.snfddal.util.StatementBuilder;
import com.suning.snfddal.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * This class represents the statement ALTER TABLE ADD CONSTRAINT
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class AlterTableAddConstraint extends SchemaCommand {

    private final boolean ifNotExists;
    private int type;
    private String constraintName;
    private String tableName;
    private IndexColumn[] indexColumns;
    private int deleteAction;
    private int updateAction;
    private Schema refSchema;
    private String refTableName;
    private IndexColumn[] refIndexColumns;
    private Expression checkExpression;
    private Index index, refIndex;
    private String comment;
    private boolean checkExisting;
    private boolean primaryKeyHash;
    private ArrayList<Index> createdIndexes = New.arrayList();

    public AlterTableAddConstraint(Session session, Schema schema, boolean ifNotExists) {
        super(session, schema);
        this.ifNotExists = ifNotExists;
    }

    private static Index getUniqueIndex(Table t, IndexColumn[] cols) {
        for (Index idx : t.getIndexes()) {
            if (canUseUniqueIndex(idx, t, cols)) {
                return idx;
            }
        }
        return null;
    }

    private static boolean canUseUniqueIndex(Index idx, Table table,
                                             IndexColumn[] cols) {
        if (idx.getTable() != table || !idx.getIndexType().isUnique()) {
            return false;
        }
        Column[] indexCols = idx.getColumns();
        if (indexCols.length > cols.length) {
            return false;
        }
        HashSet<Column> set = New.hashSet();
        for (IndexColumn c : cols) {
            set.add(c.column);
        }
        for (Column c : indexCols) {
            // all columns of the index must be part of the list,
            // but not all columns of the list need to be part of the index
            if (!set.contains(c)) {
                return false;
            }
        }
        return true;
    }

    private static Index getIndex(Table t, IndexColumn[] cols) {
        for (Index idx : t.getIndexes()) {
            if (canUseIndex(idx, t, cols)) {
                return idx;
            }
        }
        return null;
    }

    private static boolean canUseIndex(Index existingIndex, Table table,
                                       IndexColumn[] cols) {
        if (existingIndex.getTable() != table) {
            // can't use the scan index or index of another table
            return false;
        }
        Column[] indexCols = existingIndex.getColumns();
        if (indexCols.length != cols.length) {
            return false;
        }
        for (IndexColumn col : cols) {
            // all columns of the list must be part of the index
            int idx = existingIndex.getColumnIndex(col.column);
            if (idx < 0) {
                return false;
            }
        }

        return true;
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

    @Override
    public int update() {
        Database db = session.getDatabase();
        TableMate table = this.getTableMate(tableName);
        session.getUser().checkRight(table, Right.ALL);
        TableNode[] tableNodes = table.getPartitionNode();
        IndexColumn.mapColumns(indexColumns, table);
        switch (type) {
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
                IndexColumn.mapColumns(indexColumns, table);
                index = table.findPrimaryKey();
                if (index != null) {
                    // if there is an index, it must match with the one declared
                    // we don't test ascending / descending
                    IndexColumn[] pkCols = index.getIndexColumns();
                    if (pkCols.length != indexColumns.length) {
                        throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                    }
                    for (int i = 0; i < pkCols.length; i++) {
                        if (pkCols[i].column != indexColumns[i].column) {
                            throw DbException.get(ErrorCode.SECOND_PRIMARY_KEY);
                        }
                    }
                }
                if (index == null) {
                    IndexType indexType = IndexType.createPrimaryKey(false);
                    ArrayList<Column> idxColumns = New.arrayList(indexColumns.length);
                    for (IndexColumn item : indexColumns) {
                        idxColumns.add(item.column);
                    }
                    //index = table.addIndex(idxColumns, indexType);
                }
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {
                IndexColumn.mapColumns(indexColumns, table);
                if (index != null && canUseUniqueIndex(index, table, indexColumns)) {
                    index.getIndexType().setBelongsToConstraint(true);
                } else {
                    index = getUniqueIndex(table, indexColumns);
                    if (index == null) {
                        index = createIndex(table, indexColumns, true);
                    }
                }
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {

            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
                Table refTable = refSchema.getTableOrView(session, refTableName);
                TableNode[] refTableNode = table.getPartitionNode();
                Map<TableNode, TableNode> symmetryRelation = getSymmetryRelation(tableNodes, refTableNode);
                if (symmetryRelation == null) {
                    throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                            "The original table and reference table should be symmetrical.");
                }
                session.getUser().checkRight(refTable, Right.ALL);
                IndexColumn.mapColumns(indexColumns, table);
                if (index != null && canUseIndex(index, table, indexColumns)) {
                    index.getIndexType().setBelongsToConstraint(true);
                } else {
                    index = getIndex(table, indexColumns);
                    if (index == null) {
                        index = createIndex(table, indexColumns, false);
                    }
                }
                if (refIndexColumns == null) {
                    Index refIdx = refTable.getPrimaryKey();
                    refIndexColumns = refIdx.getIndexColumns();
                } else {
                    IndexColumn.mapColumns(refIndexColumns, refTable);
                }
                if (refIndexColumns.length != indexColumns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                if (refIndex != null && refIndex.getTable() == refTable &&
                        canUseIndex(refIndex, refTable, refIndexColumns)) {
                    refIndex.getIndexType().setBelongsToConstraint(true);
                } else {
                    refIndex = null;
                }
                if (refIndex == null) {
                    refIndex = getIndex(refTable, refIndexColumns);
                    if (refIndex == null) {
                        refIndex = createIndex(refTable, refIndexColumns, true);
                    }
                }
                break;
            }
            default:
                throw DbException.throwInternalError("type=" + type);
        }
        throw DbException.getUnsupportedException("TODO");
    }

    public void setDeleteAction(int action) {
        this.deleteAction = action;
    }

    public void setUpdateAction(int action) {
        this.updateAction = action;
    }

    public void setConstraintName(String constraintName) {
        this.constraintName = constraintName;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setCheckExpression(Expression expression) {
        this.checkExpression = expression;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    public void setIndexColumns(IndexColumn[] indexColumns) {
        this.indexColumns = indexColumns;
    }

    /**
     * Set the referenced table.
     *
     * @param refSchema the schema
     * @param ref       the table name
     */
    public void setRefTableName(Schema refSchema, String ref) {
        this.refSchema = refSchema;
        this.refTableName = ref;
    }

    public void setRefIndexColumns(IndexColumn[] indexColumns) {
        this.refIndexColumns = indexColumns;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public void setRefIndex(Index refIndex) {
        this.refIndex = refIndex;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setCheckExisting(boolean b) {
        this.checkExisting = b;
    }

    public void setPrimaryKeyHash(boolean b) {
        this.primaryKeyHash = b;
    }

    @Override
    public String getPlanSQL() {
        return getCreateSQLForCopy(tableName, constraintName);
    }

    public String getCreateSQLForCopy(String tableName, String constraintName) {
        switch (type) {
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
                StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
                buff.append(tableName).append(" ADD CONSTRAINT ");
                if (ifNotExists) {
                    buff.append("IF NOT EXISTS ");
                }
                buff.append(constraintName);
                if (comment != null) {
                    buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
                }
                buff.append(' ').append("PRIMARY KEY").append('(');
                for (IndexColumn c : indexColumns) {
                    buff.appendExceptFirst(", ");
                    buff.append(Parser.quoteIdentifier(c.column.getName()));
                }
                buff.append(')');
                if (index != null) {
                    buff.append(" INDEX ").append(index.getSQL());
                }
                return buff.toString();

            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {

            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {

            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {

            }
            default:
                throw DbException.throwInternalError("type=" + type);
        }

    }

    /**
     * Create a unique constraint name.
     *
     * @param session the session
     * @param table   the constraint table
     * @return the unique name
     */
    public String getUniqueConstraintName(Session session, Table table) {
        return null;
    }

    private Index createIndex(Table t, IndexColumn[] cols, boolean unique) {
        int indexId = getObjectId();
        IndexType indexType;
        if (unique) {
            // for unique constraints
            indexType = IndexType.createUnique(false);
        } else {
            // constraints
            indexType = IndexType.createNonUnique();
        }
        indexType.setBelongsToConstraint(true);
        String prefix = constraintName == null ? "CONSTRAINT" : constraintName;
        String indexName = t.getSchema().getUniqueIndexName(session, t,
                prefix + "_INDEX_");

        ArrayList<Column> idxColumns = New.arrayList(indexColumns.length);
        for (IndexColumn item : indexColumns) {
            idxColumns.add(item.column);
        }
        //Index index = t.addIndex(idxColumns, indexType);
        //createdIndexes.add(index);
        //return index;
        return null;
    }

}
