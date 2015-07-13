/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.command.ddl;

import com.suning.snfddal.command.CommandInterface;
import com.suning.snfddal.dbobject.schema.Schema;
import com.suning.snfddal.dbobject.table.IndexColumn;
import com.suning.snfddal.engine.Session;
import com.suning.snfddal.message.DbException;

/**
 * This class represents the statement
 * CREATE INDEX
 */
public class CreateIndex extends SchemaCommand {

    private String tableName;
    private String indexName;
    private IndexColumn[] indexColumns;
    private boolean primaryKey, unique, hash, spatial;
    private boolean ifNotExists;
    private String comment;

    public CreateIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexColumns(IndexColumn[] columns) {
        this.indexColumns = columns;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setPrimaryKey(boolean b) {
        this.primaryKey = b;
    }

    public void setUnique(boolean b) {
        this.unique = b;
    }

    public void setHash(boolean b) {
        this.hash = b;
    }

    public void setSpatial(boolean b) {
        this.spatial = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_INDEX;
    }

}
