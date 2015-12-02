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
package com.wplatform.ddal.dbobject.index;


/**
 * Represents information about the properties of an index
 */
public class IndexType {

    private boolean shardingKey, primaryKey, unique, hash, scan, spatial;

    /**
     * Create a primary key index.
     * @param hash       if a hash index should be used
     * @return the index type
     */
    public static IndexType createShardingKey(boolean hash) {
        IndexType type = new IndexType();
        type.shardingKey = true;
        type.primaryKey = false;
        type.hash = hash;
        type.unique = true;
        return type;
    }
    /**
     * Create a primary key index.
     * @param hash       if a hash index should be used
     * @return the index type
     */
    public static IndexType createPrimaryKey(boolean hash) {
        IndexType type = new IndexType();
        type.primaryKey = true;
        type.hash = hash;
        type.unique = true;
        return type;
    }

    /**
     * Create a unique index.
     *
     * @param hash       if a hash index should be used
     * @return the index type
     */
    public static IndexType createUnique(boolean hash) {
        IndexType type = new IndexType();
        type.unique = true;
        type.hash = hash;
        return type;
    }

    /**
     * Create a non-unique index.
     *
     * @return the index type
     */
    public static IndexType createNonUnique() {
        return createNonUnique(false, false);
    }

    /**
     * Create a non-unique index.
     *
     * @param hash       if a hash index should be used
     * @param spatial    if a spatial index should be used
     * @return the index type
     */
    public static IndexType createNonUnique(boolean hash,
                                            boolean spatial) {
        IndexType type = new IndexType();
        type.hash = hash;
        type.spatial = spatial;
        return type;
    }

    /**
     * Create a scan pseudo-index.
     *
     * @return the index type
     */
    public static IndexType createScan() {
        IndexType type = new IndexType();
        type.scan = true;
        return type;
    }

    /**
     * Is this a hash index?
     *
     * @return true if it is a hash index
     */
    public boolean isHash() {
        return hash;
    }

    /**
     * Is this a spatial index?
     *
     * @return true if it is a spatial index
     */
    public boolean isSpatial() {
        return spatial;
    }

    /**
     * Does this index belong to a primary key constraint?
     *
     * @return true if it references a primary key constraint
     */
    public boolean isShardingKey() {
        return shardingKey;
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
     * Is this a table scan pseudo-index?
     *
     * @return true if it is
     */
    public boolean isScan() {
        return scan;
    }

}
