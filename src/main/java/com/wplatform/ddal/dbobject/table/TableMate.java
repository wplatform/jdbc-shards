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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import com.wplatform.ddal.command.ddl.CreateTableData;
import com.wplatform.ddal.dbobject.index.Index;
import com.wplatform.ddal.dbobject.index.IndexMate;
import com.wplatform.ddal.dbobject.index.IndexType;
import com.wplatform.ddal.dbobject.schema.Schema;
import com.wplatform.ddal.dispatch.rule.RoutingResult;
import com.wplatform.ddal.dispatch.rule.RuleColumn;
import com.wplatform.ddal.dispatch.rule.TableNode;
import com.wplatform.ddal.dispatch.rule.TableRouter;
import com.wplatform.ddal.engine.Session;
import com.wplatform.ddal.excutor.Optional;
import com.wplatform.ddal.message.DbException;
import com.wplatform.ddal.message.ErrorCode;
import com.wplatform.ddal.shards.DataSourceRepository;
import com.wplatform.ddal.util.JdbcUtils;
import com.wplatform.ddal.util.MathUtils;
import com.wplatform.ddal.util.New;
import com.wplatform.ddal.util.StringUtils;
import com.wplatform.ddal.value.DataType;
import com.wplatform.ddal.value.ValueDate;
import com.wplatform.ddal.value.ValueTime;
import com.wplatform.ddal.value.ValueTimestamp;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableMate extends Table {

    private static final int MAX_RETRY = 2;
    private static final long ROW_COUNT_APPROXIMATION = 100000;

    private final boolean globalTemporary;
    private final ArrayList<Index> indexes = New.arrayList();
    private Index scanIndex;

    private TableRouter tableRouter;
    private TableNode[] shards;
    private int scanLevel;

    private DbException initException;
    private boolean storesLowerCase;
    private boolean storesMixedCase;
    private boolean storesMixedCaseQuoted;
    private boolean supportsMixedCaseIdentifiers;

    public TableMate(CreateTableData data) {
        super(data.schema, data.id, data.tableName);
        this.globalTemporary = data.globalTemporary;
        setTemporary(data.temporary);
        Column[] cols = new Column[data.columns.size()];
        data.columns.toArray(cols);
        setColumns(cols);
        scanIndex = new IndexMate(this, data.id, null, IndexColumn.wrap(cols), IndexType.createScan());
        indexes.add(scanIndex);
    }

    public TableMate(Schema schema, int id, String name) {
        super(schema, id, name);
        this.globalTemporary = false;
    }
    /**
     * @return the tableRouter
     */
    public TableRouter getTableRouter() {
        return tableRouter;
    }

    /**
     * @param tableRouter the tableRouter to set
     */
    public void setTableRouter(TableRouter tableRouter) {
        this.tableRouter = tableRouter;
    }

    /**
     * @return the scanLevel
     */
    public int getScanLevel() {
        return scanLevel;
    }

    /**
     * @param scanLevel the scanLevel to set
     */
    public void setScanLevel(int scanLevel) {
        this.scanLevel = scanLevel;
    }

    /**
     * @return the shards
     */
    public TableNode[] getShards() {
        return shards;
    }

    /**
     * test the table is global table.
     * @return
     */
    public boolean isReplication() {
        return shards != null && shards.length > 1;
    }

    /**
     * @param shards the shards to set
     */
    public void setShards(TableNode[] shards) {
        this.shards = shards;
    }

    /**
     * @return
     * @see com.wplatform.ddal.dispatch.rule.TableRouter#getPartition()
     */
    public TableNode[] getPartitionNode() {
        if (tableRouter != null) {
            List<TableNode> partition = tableRouter.getPartition();
            return partition.toArray(new TableNode[partition.size()]);
        }
        return shards;

    }

    /**
     * validation the rule columns is in the table columns
     */
    private void validationRuleColumn(Column[] columns) {
        if (tableRouter != null) {
            for (RuleColumn ruleCol : tableRouter.getRuleColumns()) {
                Column matched = null;
                for (Column column : columns) {
                    String colName = column.getName();
                    if (colName.equalsIgnoreCase(ruleCol.getName())) {
                        matched = column;
                        break;
                    }
                }
                if (matched == null) {
                    throw DbException.throwInternalError(
                            "The rule column " + ruleCol + " does not exist in " + getName() + " table.");
                }
            }
        }
    }

    public void check() {
        if (initException != null) {
            Column[] cols = {};
            setColumns(cols);
            indexes.clear();
            throw initException;
        }
    }

    public boolean isInited() {
        return initException != null;
    }

    public void markDeleted() {
        Column[] cols = {};
        setColumns(cols);
        indexes.clear();
        initException = DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, this.getSQL());
    }

    @Override
    public boolean isGlobalTemporary() {
        return globalTemporary;
    }

    @Override
    public String getTableType() {
        return Table.TABLE;
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        if (this.tableRouter == null) {
            return ROW_COUNT_APPROXIMATION;
        } else {
            return tableRouter.getPartition().size() * ROW_COUNT_APPROXIMATION;
        }

    }

    @Override
    public void checkRename() {

    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    private Index addIndex(ArrayList<Column> list, IndexType indexType) {
        Column[] cols = new Column[list.size()];
        list.toArray(cols);
        Index index = new IndexMate(this, 0, null, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
        return index;
    }

    public void loadMataData(Session session) {
        TableNode[] nodes = getPartitionNode();
        if (nodes == null || nodes.length < 1) {
            throw new IllegalStateException();
        }
        TableNode matadataNode = nodes[0];
        String tableName = matadataNode.getCompositeObjectName();
        String shardName = matadataNode.getShardName();
        try {
            trace.debug("Try to load {0} metadata from table {1}.{2}", getName(), shardName, tableName);
            readMataData(session, matadataNode);
            trace.debug("Load the {0} metadata success.", getName());
            initException = null;
        } catch (DbException e) {
            trace.debug("Fail to load {0} metadata from table {1}.{2}. error: {3}", getName(), shardName, tableName,
                    e.getCause().getMessage());
            initException = e;
            Column[] cols = {};
            setColumns(cols);
            scanIndex = new IndexMate(this, 0, null, IndexColumn.wrap(cols), IndexType.createNonUnique());
            indexes.add(scanIndex);
        }
    }

    /**
     * @param session
     */
    public void readMataData(Session session, TableNode matadataNode) {
        for (int retry = 0;; retry++) {
            try {
                Connection conn = null;
                String tableName = matadataNode.getCompositeObjectName();
                String shardName = matadataNode.getShardName();
                try {
                    DataSourceRepository dsRepository = session.getDataSourceRepository();
                    DataSource dataSource = dsRepository.getDataSourceByShardName(shardName);
                    Optional optional = Optional.build().shardName(shardName).readOnly(false);
                    conn = session.applyConnection(dataSource, optional);
                    tableName = database.identifier(tableName);
                    tryReadMetaData(conn, tableName);
                    return;
                } catch (Exception e) {
                    throw DbException.convert(e);
                } finally {
                    JdbcUtils.closeSilently(conn);
                }
            } catch (DbException e) {
                if (retry >= MAX_RETRY) {
                    throw e;
                }
            }
        }

    }

    private void tryReadMetaData(Connection conn, String tableName) throws SQLException {

        DatabaseMetaData meta = conn.getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        storesMixedCaseQuoted = meta.storesMixedCaseQuotedIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();

        ResultSet rs = meta.getTables(null, null, tableName, null);
        if (rs.next() && rs.next()) {
            throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH, tableName);
        }
        rs.close();
        rs = meta.getColumns(null, null, tableName, null);
        int i = 0;
        ArrayList<Column> columnList = New.arrayList();
        HashMap<String, Column> columnMap = New.hashMap();
        String catalog = null, schema = null;
        while (rs.next()) {
            String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                columnMap.clear();
                columnList.clear();
                break;
            }
            String n = rs.getString("COLUMN_NAME");
            n = convertColumnName(n);
            int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            int scale = rs.getInt("DECIMAL_DIGITS");
            scale = convertScale(sqlType, scale);
            int displaySize = MathUtils.convertLongToInt(precision);
            int type = DataType.convertSQLTypeToValueType(sqlType);
            Column col = new Column(n, type, precision, scale, displaySize);
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        rs.close();
        // check if the table is accessible
        Statement stat = null;
        try {
            stat = conn.createStatement();
            rs = stat.executeQuery("SELECT * FROM " + tableName + " T WHERE 1=0");
            if (columnList.size() == 0) {
                // alternative solution
                ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    int scale = rsMeta.getScale(i + 1);
                    scale = convertScale(sqlType, scale);
                    int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    int type = DataType.getValueTypeFromResultSet(rsMeta, i + 1);
                    Column col = new Column(n, type, precision, scale, displaySize);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        } catch (Exception e) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e, tableName + "(" + e.toString() + ")");
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        Column[] cols = new Column[columnList.size()];
        columnList.toArray(cols);
        validationRuleColumn(cols);
        setColumns(cols);
        // create scan index
        int id = getId();
        scanIndex = new IndexMate(this, id, null, IndexColumn.wrap(cols), IndexType.createNonUnique());
        indexes.add(scanIndex);
        // create shardingKey index
        if (tableRouter != null) {
            ArrayList<Column> shardCol = New.arrayList();
            for (RuleColumn ruleCol : tableRouter.getRuleColumns()) {
                for (Column column : columns) {
                    String colName = column.getName();
                    if (colName.equalsIgnoreCase(ruleCol.getName())) {
                        shardCol.add(column);
                    }
                }
            }
            addIndex(shardCol, IndexType.createShardingKey(false));
        }

        // load primary keys
        try {
            rs = meta.getPrimaryKeys(null, null, tableName);
        } catch (Exception e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ArrayList<Column> list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = New.arrayList();
            do {
                int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                if (idx == 0) {
                    // workaround for a bug in the SQLite JDBC driver
                    list.add(column);
                } else {
                    list.set(idx - 1, column);
                }
            } while (rs.next());
            addIndex(list, IndexType.createPrimaryKey(false));
            rs.close();
        }

        try {
            rs = meta.getIndexInfo(null, null, tableName, false, true);
        } catch (Exception e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = New.arrayList();
        IndexType indexType = null;
        if (rs != null) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
                String newIndex = rs.getString("INDEX_NAME");
                if (pkName.equals(newIndex)) {
                    continue;
                }
                if (indexName != null && !indexName.equals(newIndex)) {
                    addIndex(list, indexType);
                    indexName = null;
                }
                if (indexName == null) {
                    indexName = newIndex;
                    list.clear();
                }
                boolean unique = !rs.getBoolean("NON_UNIQUE");
                indexType = unique ? IndexType.createUnique(false) : IndexType.createNonUnique();
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                list.add(column);
            }
            rs.close();
        }
        if (indexName != null) {
            addIndex(list, indexType);
        }
    }

    private String convertColumnName(String columnName) {
        if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && storesMixedCaseQuoted) {
            // MS SQL Server (identifiers are case insensitive even if quoted)
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private static long convertPrecision(int sqlType, long precision) {
        // workaround for an Oracle problem:
        // for DATE columns, the reported precision is 7
        // for DECIMAL columns, the reported precision is 0
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (precision == 0) {
                precision = 65535;
            }
            break;
        case Types.DATE:
            precision = Math.max(ValueDate.PRECISION, precision);
            break;
        case Types.TIMESTAMP:
            precision = Math.max(ValueTimestamp.PRECISION, precision);
            break;
        case Types.TIME:
            precision = Math.max(ValueTime.PRECISION, precision);
            break;
        }
        return precision;
    }

    private static int convertScale(int sqlType, int scale) {
        // workaround for an Oracle problem:
        // for DECIMAL columns, the reported precision is -127
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (scale < 0) {
                scale = 32767;
            }
            break;
        }
        return scale;
    }

    public boolean isRelationSymmetry(TableMate o) {
        if (this == o) {
            return true;
        }
        if (o == null)  {
            return false;
        }
        if(tableRouter != null) {
            if(o.tableRouter != null) {
                return tableRouter.equals(o.tableRouter);
            } else {
                return isNodeMatch(getPartitionNode(), o.getPartitionNode());
            }
        }  else {
            return isNodeMatch(getPartitionNode(), o.getPartitionNode());
        }

    }

    private static boolean isNodeMatch(TableNode[] nodes1, TableNode[] nodes2) {
        TableNode[] group1 = RoutingResult.fixedResult(nodes1).group();
        TableNode[] group2 = RoutingResult.fixedResult(nodes2).group();
        for (TableNode tableNode1 : group1) {
            boolean isMatched = false;
            for (TableNode tableNode2 : group2) {
                if (tableNode1.getShardName().equals(tableNode2.getShardName())) {
                    isMatched = true;
                    break;
                }
            }
            if (!isMatched) {
                return false;
            }
        }
        return true;
    }

}
