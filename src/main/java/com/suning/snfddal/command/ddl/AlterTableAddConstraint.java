/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import java.util.ArrayList;

import com.suning.snfddal.command.expression.Expression;
import com.suning.snfddal.dbobject.index.Index;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;
import com.suning.snfddal.util.New;

/**
 * This class represents the statement ALTER TABLE ADD CONSTRAINT
 */
public class AlterTableAddConstraint extends SchemaCommand {

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
    private final boolean ifNotExists;
    private ArrayList<Index> createdIndexes = New.arrayList();

    public AlterTableAddConstraint(Session session, Schema schema, boolean ifNotExists) {
        super(session, schema);
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
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

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setCheckExpression(Expression expression) {
        this.checkExpression = expression;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIndexColumns(IndexColumn[] indexColumns) {
        this.indexColumns = indexColumns;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    /**
     * Set the referenced table.
     *
     * @param refSchema the schema
     * @param ref the table name
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

}
