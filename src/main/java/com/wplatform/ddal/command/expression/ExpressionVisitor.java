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
package com.wplatform.ddal.command.expression;

import java.util.HashSet;

import com.wplatform.ddal.dbobject.DbObject;
import com.wplatform.ddal.dbobject.table.Column;
import com.wplatform.ddal.dbobject.table.ColumnResolver;
import com.wplatform.ddal.dbobject.table.Table;

/**
 * The visitor pattern is used to iterate through all expressions of a query
 * to optimize a statement.
 */
public class ExpressionVisitor {

    /**
     * Is the value independent on unset parameters or on columns of a higher
     * level query, or sequence values (that means can it be evaluated right
     * now)?
     */
    public static final int INDEPENDENT = 0;

    /**
     * The visitor singleton for the type INDEPENDENT.
     */
    public static final ExpressionVisitor INDEPENDENT_VISITOR =
            new ExpressionVisitor(INDEPENDENT);

    /**
     * Are all aggregates MIN(column), MAX(column), or COUNT(*) for the given
     * table (getTable)?
     */
    public static final int OPTIMIZABLE_MIN_MAX_COUNT_ALL = 1;

    /**
     * Does the expression return the same results for the same parameters?
     */
    public static final int DETERMINISTIC = 2;

    /**
     * The visitor singleton for the type DETERMINISTIC.
     */
    public static final ExpressionVisitor DETERMINISTIC_VISITOR =
            new ExpressionVisitor(DETERMINISTIC);

    /**
     * Can the expression be evaluated, that means are all columns set to
     * 'evaluatable'?
     */
    public static final int EVALUATABLE = 3;

    /**
     * The visitor singleton for the type EVALUATABLE.
     */
    public static final ExpressionVisitor EVALUATABLE_VISITOR =
            new ExpressionVisitor(EVALUATABLE);

    /**
     * Request to set the latest modification id (addDataModificationId).
     */
    public static final int SET_MAX_DATA_MODIFICATION_ID = 4;

    /**
     * Does the expression have no side effects (change the data)?
     */
    public static final int READONLY = 5;

    /**
     * The visitor singleton for the type EVALUATABLE.
     */
    public static final ExpressionVisitor READONLY_VISITOR =
            new ExpressionVisitor(READONLY);

    /**
     * Does an expression have no relation to the given table filter
     * (getResolver)?
     */
    public static final int NOT_FROM_RESOLVER = 6;

    /**
     * Request to get the set of dependencies (addDependency).
     */
    public static final int GET_DEPENDENCIES = 7;

    /**
     * Can the expression be added to a condition of an outer query. Example:
     * ROWNUM() can't be added as a condition to the inner query of select id
     * from (select t.*, rownum as r from test t) where r between 2 and 3; Also
     * a sequence expression must not be used.
     */
    public static final int QUERY_COMPARABLE = 8;

    /**
     * Get all referenced columns.
     */
    public static final int GET_COLUMNS = 9;


    /**
     * Get all referenced columns.
     */
    public static final int EXPORT_PARAMETER = 9;

    /**
     * The visitor singleton for the type QUERY_COMPARABLE.
     */
    public static final ExpressionVisitor QUERY_COMPARABLE_VISITOR =
            new ExpressionVisitor(QUERY_COMPARABLE);

    private final int type;
    private final int queryLevel;
    private final HashSet<DbObject> dependencies;
    private final HashSet<Column> columns;
    private final Table table;
    private final ColumnResolver resolver;

    private ExpressionVisitor(int type,
                              int queryLevel,
                              HashSet<DbObject> dependencies,
                              HashSet<Column> columns,
                              Table table, ColumnResolver resolver) {
        this.type = type;
        this.queryLevel = queryLevel;
        this.dependencies = dependencies;
        this.columns = columns;
        this.table = table;
        this.resolver = resolver;
    }

    private ExpressionVisitor(int type) {
        this.type = type;
        this.queryLevel = 0;
        this.dependencies = null;
        this.columns = null;
        this.table = null;
        this.resolver = null;
    }

    /**
     * Create a new visitor object to collect dependencies.
     *
     * @param dependencies the dependencies set
     * @return the new visitor
     */
    public static ExpressionVisitor getDependenciesVisitor(
            HashSet<DbObject> dependencies) {
        return new ExpressionVisitor(GET_DEPENDENCIES, 0, dependencies, null,
                null, null);
    }

    /**
     * Create a new visitor to check if all aggregates are for the given table.
     *
     * @param table the table
     * @return the new visitor
     */
    public static ExpressionVisitor getOptimizableVisitor(Table table) {
        return new ExpressionVisitor(OPTIMIZABLE_MIN_MAX_COUNT_ALL, 0, null,
                null, table, null);
    }

    /**
     * Create a new visitor to check if no expression depends on the given
     * resolver.
     *
     * @param resolver the resolver
     * @return the new visitor
     */
    static ExpressionVisitor getNotFromResolverVisitor(ColumnResolver resolver) {
        return new ExpressionVisitor(NOT_FROM_RESOLVER, 0, null, null, null,
                resolver);
    }

    /**
     * Create a new visitor to get all referenced columns.
     *
     * @param columns the columns map
     * @return the new visitor
     */
    public static ExpressionVisitor getColumnsVisitor(HashSet<Column> columns) {
        return new ExpressionVisitor(GET_COLUMNS, 0, null, columns, null, null);
    }

    public static ExpressionVisitor getMaxModificationIdVisitor() {
        return new ExpressionVisitor(SET_MAX_DATA_MODIFICATION_ID, 0, null,
                null, null, null);
    }

    public static ExpressionVisitor getExportParameterVisitor() {
        return new ExpressionVisitor(EXPORT_PARAMETER, 0, null,
                null, null, null);
    }

    /**
     * Add a new dependency to the set of dependencies.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @param obj the additional dependency.
     */
    public void addDependency(DbObject obj) {
        dependencies.add(obj);
    }

    /**
     * Add a new column to the set of columns.
     * This is used for GET_COLUMNS visitors.
     *
     * @param column the additional column.
     */
    void addColumn(Column column) {
        columns.add(column);
    }

    /**
     * Get the dependency set.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @return the set
     */
    public HashSet<DbObject> getDependencies() {
        return dependencies;
    }

    /**
     * Increment or decrement the query level.
     *
     * @param offset 1 to increment, -1 to decrement
     * @return a clone of this expression visitor, with the changed query level
     */
    public ExpressionVisitor incrementQueryLevel(int offset) {
        return new ExpressionVisitor(type, queryLevel + offset, dependencies,
                columns, table, resolver);
    }

    /**
     * Get the column resolver.
     * This is used for NOT_FROM_RESOLVER visitors.
     *
     * @return the column resolver
     */
    public ColumnResolver getResolver() {
        return resolver;
    }

    int getQueryLevel() {
        return queryLevel;
    }

    /**
     * Get the table.
     * This is used for OPTIMIZABLE_MIN_MAX_COUNT_ALL visitors.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get the visitor type.
     *
     * @return the type
     */
    public int getType() {
        return type;
    }

}
