/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.suning.snfddal.dbobject.index;


/**
 * Represents information about the properties of an index
 */
public class IndexType {

    private boolean partitionKey, primaryKey, unique, scan;
    
    /**
     * Create a partitionKey key index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createPartitionKey() {
        IndexType type = new IndexType();
        type.partitionKey = true;
        type.primaryKey = true;
        type.unique = true;
        return type;
    }
    
    /**
     * Create a primary key index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createPrimaryKey() {
        IndexType type = new IndexType();
        type.primaryKey = true;
        type.unique = true;
        return type;
    }

    /**
     * Create a unique index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @return the index type
     */
    public static IndexType createUnique() {
        IndexType type = new IndexType();
        type.unique = true;
        return type;
    }

    /**
     * Create a non-unique index.
     *
     * @param persistent if the index is persistent
     * @param hash if a hash index should be used
     * @param spatial if a spatial index should be used
     * @return the index type
     */
    public static IndexType createNonUnique() {
        IndexType type = new IndexType();
        return type;
    }
    
    /**
     * Create a scan pseudo-index.
     *
     * @param persistent if the index is persistent
     * @return the index type
     */
    public static IndexType createScan() {
        IndexType type = new IndexType();
        type.scan = true;
        return type;
    }
    
    /**
     * Does this index belong to a primary key constraint?
     *
     * @return true if it references a primary key constraint
     */
    public boolean isPartitionKey() {
        return partitionKey;
    }

    /**
     * Does this index belong to a primary key constraint?
     *
     * @return true if it references a primary key constraint
     */
    public boolean isPrimaryKey() {
        return primaryKey;
    }

    /**
     * Is this a unique index?
     *
     * @return true if it is
     */
    public boolean isUnique() {
        return unique;
    }

    /**
     * Get the SQL snippet to create such an index.
     *
     * @return the SQL snippet
     */
    public String getSQL() {
        StringBuilder buff = new StringBuilder();
        if (primaryKey) {
            buff.append("PRIMARY KEY");
        } else {
            if (unique) {
                buff.append("UNIQUE ");
            }
            buff.append("INDEX");
        }
        return buff.toString();
    }

    /**
     * Is this a table scan pseudo-index?
     *
     * @return true if it is
     */
    public boolean isScan() {
        return scan;
    }

}
