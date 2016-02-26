/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.DbCompareReport.TableReport;
import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DbCompare has the ability to compare two SQL-based datasources and output a report of
 * of differences, and optionally SQL to bring the target into sync with the source. 
 */
public class DbCompare {

    final Logger log = LoggerFactory.getLogger(getClass());
    
    ISqlRowMapper<Row> defaultRowMapper = new ISqlRowMapper<Row>() {
        @Override
        public Row mapRow(Row row) {
            return row;
        }
    };   

    private ISymmetricEngine sourceEngine;
    private ISymmetricEngine targetEngine;

    private OutputStream sqlDiffStream;
    private List<String> includedTableNames;
    private List<String> excludedTableNames;
    private boolean useSymmetricConfig = true;
    private DbValueComparator dbValueComparator;

    public DbCompare(ISymmetricEngine sourceEngine, ISymmetricEngine targetEngine) {
        this.sourceEngine = sourceEngine;
        this.targetEngine = targetEngine;
        dbValueComparator = new DbValueComparator(sourceEngine, targetEngine);
    }

    public DbCompareReport compare() {
        DbCompareReport report = new DbCompareReport();
        Map<Table, Table> tables = getTablesToCompare();

        for (Table sourceTable : tables.keySet()) {
            Table targetTable = tables.get(sourceTable);
            TableReport tableReport = compareTables(sourceTable, targetTable);
            report.addTableReport(tableReport);
        }

        return report;
    }

    protected TableReport compareTables(Table sourceTable, Table targetTable) {
        String sourceSelect = getComparisonSQL(sourceTable, sourceEngine.getDatabasePlatform());
        String targetSelect = getComparisonSQL(targetTable, targetEngine.getDatabasePlatform());

        CountingSqlReadCursor sourceCursor = new CountingSqlReadCursor(sourceEngine.getDatabasePlatform().
                getSqlTemplate().queryForCursor(sourceSelect, defaultRowMapper));
        CountingSqlReadCursor targetCursor = new CountingSqlReadCursor(targetEngine.getDatabasePlatform().
                getSqlTemplate().queryForCursor(targetSelect, defaultRowMapper));

        TableReport tableReport = new TableReport();
        tableReport.setSourceTable(sourceTable.getFullyQualifiedTableName());
        tableReport.setTargetTable(targetTable.getFullyQualifiedTableName());

        Row sourceRow = sourceCursor.next();
        Row targetRow = targetCursor.next();
        
        int counter = 0;
        long startTime = System.currentTimeMillis();

        while (true) {  
            if (sourceRow == null && targetRow == null) {
                break;
            }
            
            counter++;
            if ((counter % 10000) == 0) {
                log.info("{} rows processed for table {}. Elapsed time {}.", 
                        counter, sourceTable.getName(), (System.currentTimeMillis()-startTime));
            }

            DbCompareRow sourceCompareRow = sourceRow != null ? 
                    new DbCompareRow(sourceEngine, dbValueComparator, sourceTable, sourceRow) : null;
            DbCompareRow targetCompareRow = targetRow != null ? 
                    new DbCompareRow(targetEngine, dbValueComparator,  targetTable, targetRow) : null;

            int comparePk = comparePk(sourceCompareRow, targetCompareRow);
            if (comparePk == 0) {
                Map<Column, String> deltas = sourceCompareRow.compareTo(targetCompareRow);
                if (deltas.isEmpty()) {
                    tableReport.countMatchedRow();                    
                } else {
                    writeUpdate(targetCompareRow, deltas);
                    tableReport.countDifferentRow();
                }

                sourceRow = sourceCursor.next();
                targetRow = targetCursor.next();
            } else if (comparePk < 0) {
                writeInsert(sourceCompareRow, targetTable);
                tableReport.countMissingRow();
                sourceRow = sourceCursor.next();
            } else {
                writeDelete(targetCompareRow);
                tableReport.countExtraRow();
                targetRow = targetCursor.next();
            }
        }

        tableReport.setSourceRows(sourceCursor.count);
        tableReport.setTargetRows(targetCursor.count);

        return tableReport;
    }

    protected void writeDelete(DbCompareRow targetCompareRow) {
        if (sqlDiffStream == null) {
            return;
        }

        Table table = targetCompareRow.getTable();

        DmlStatement statement =  targetEngine.getDatabasePlatform().createDmlStatement(DmlType.DELETE,
                table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), null,
                null, null);

        Row row = new Row(targetCompareRow.getTable().getPrimaryKeyColumnCount());

        for (int i = 0; i < targetCompareRow.getTable().getPrimaryKeyColumnCount(); i++) {
            row.put(table.getColumn(i).getName(), 
                    targetCompareRow.getRowValues().get(targetCompareRow.getTable().getColumn(i).getName()));
        }

        String sql = statement.buildDynamicDeleteSql(BinaryEncoding.HEX, row, false, true);

        writeStatement(sql);
    }

    protected void writeInsert(DbCompareRow sourceCompareRow, Table targetTable) { 
        if (sqlDiffStream == null) {
            return;
        }

        DmlStatement statement =  targetEngine.getDatabasePlatform().createDmlStatement(DmlType.INSERT,
                targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                targetTable.getPrimaryKeyColumns(), targetTable.getColumns(),
                null, null);

        Row row = new Row(targetTable.getColumnCount());

        for (int i = 0; i < targetTable.getColumnCount(); i++) {
            row.put(targetTable.getColumn(i).getName(), 
                    sourceCompareRow.getRowValues().get(sourceCompareRow.getTable().getColumn(i).getName())); // TODO cleaner TableA->TableB mapping would be nice.
        }

        String sql = statement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);

        writeStatement(sql);
    }

    /**
     * @param targetCompareRow
     * @param deltas
     */
    protected void writeUpdate(DbCompareRow targetCompareRow, Map<Column, String> deltas) { 
        if (sqlDiffStream == null) {
            return;
        }

        Table table = targetCompareRow.getTable();

        Column[] changedColumns = deltas.keySet().toArray(new Column[deltas.keySet().size()]);

        DmlStatement statement = targetEngine.getDatabasePlatform().createDmlStatement(DmlType.UPDATE,
                table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), changedColumns,
                null, null);

        Row row = new Row(changedColumns.length+table.getPrimaryKeyColumnCount());
        for (Column changedColumn : deltas.keySet()) {
            String value = deltas.get(changedColumn);
            row.put(changedColumn.getName(), value);
        }
        for (String pkColumnName : table.getPrimaryKeyColumnNames()) {
            String value = targetCompareRow.getRow().getString(pkColumnName);
            row.put(pkColumnName, value);
        }
        String sql = statement.buildDynamicSql(BinaryEncoding.HEX, row, false, true);

        writeStatement(sql);
    }

    protected void writeStatement(String statement) {
        try {
            sqlDiffStream.write(statement.getBytes()); 
            sqlDiffStream.write("\r\n".getBytes());
        } catch (Exception ex) {
            throw new RuntimeException("failed to write to sqlDiffStream.", ex);
        }
    }

    protected int comparePk(DbCompareRow sourceCompareRow, DbCompareRow targetCompareRow) {
        if (sourceCompareRow != null && targetCompareRow == null) {
            return -1;
        }
        if (sourceCompareRow == null && targetCompareRow != null) {
            return 1;
        }

        return sourceCompareRow.comparePks(targetCompareRow);
    }

    protected String getComparisonSQL(Table table, IDatabasePlatform platform) {
        DmlStatement statement = platform.createDmlStatement(DmlType.SELECT,
                table.getCatalog(), table.getSchema(), table.getName(),
                null, table.getColumns(),
                null, null);

        StringBuilder sql = new StringBuilder(statement.getSql());
        sql.append("1=1 ");
        sql.append(buildOrderBy(table, platform));

        return sql.toString();
    }

    protected String buildOrderBy(Table table, IDatabasePlatform platform) {
        DatabaseInfo databaseInfo = platform.getDatabaseInfo();
        String quote = databaseInfo.getDelimiterToken() == null ? "" : databaseInfo.getDelimiterToken(); 
        StringBuilder orderByClause = new StringBuilder("ORDER BY ");
        for (Column pkColumn : table.getPrimaryKeyColumns()) {
            String columnName = pkColumn.getName();        
            orderByClause.append(quote).append(columnName).append(quote).append(",");
        }
        orderByClause.setLength(orderByClause.length()-1);
        return orderByClause.toString();
    }

    protected Map<Table, Table> getTablesToCompare() {
        Map<Table, Table> tablesToCompare;
        if (useSymmetricConfig) {
            tablesToCompare = loadTablesFromConfig();
        } else {
            tablesToCompare = loadTablesFromArguments();
        }

        tablesToCompare = removeExcludedTables(tablesToCompare);
        return tablesToCompare;
    }


    protected Map<Table, Table> loadTablesFromConfig() {        
        List<Trigger> triggers = sourceEngine.getTriggerRouterService().getTriggersForCurrentNode(true);
        List<String> configTables = TableConstants.getConfigTables(sourceEngine.getTablePrefix());

        List<String> tableNames = new ArrayList<String>();

        for (Trigger trigger : triggers) {
            if (!configTables.contains(trigger.getFullyQualifiedSourceTableName())) {
                tableNames.add(trigger.getFullyQualifiedSourceTableName());
            }
        }

        return loadTables(tableNames);
    }

    protected Map<Table, Table> loadTables(List<String> tableNames) {
        Map<Table, Table> tablesFromConfig = new LinkedHashMap<Table, Table>();

        for (String tableName : tableNames) {
            Table sourceTable = sourceEngine.getDatabasePlatform().getTableFromCache(tableName, true);
            if (sourceTable == null) {
                log.warn("No source table found for table name {}", tableName);
                continue;
            }
            if (sourceTable.getPrimaryKeyColumnCount() == 0) {
                log.warn("Source table {} doesn't have any primary key columns and will not be considered in the comparison.", sourceTable);
                continue;                
            }
            Table targetTable = loadTargetTable(sourceTable);
            if (targetTable == null) {
                log.warn("No target table found for table {}", tableName);
            } else if (targetTable.getPrimaryKeyColumnCount() == 0) {
                log.warn("Target table {} doesn't have any primary key columns and will not be considered in the comparison.", targetTable);
                continue;                
            }            
            tablesFromConfig.put(sourceTable, targetTable);
        }

        return tablesFromConfig;        
    }

    protected Table loadTargetTable(Table sourceTable) {
        if (useSymmetricConfig) {
            TransformTableNodeGroupLink transform = getTransformFor(sourceTable);
            if (transform != null) {
                return loadTargetTableUsingTransform(sourceTable, transform);                
            }
        } 

        Table targetTable = targetEngine.getDatabasePlatform().
                getTableFromCache(sourceTable.getName(), true);
        alignColumns(sourceTable, targetTable);

        return targetTable;
    }

    protected void alignColumns(Table sourceTable, Table targetTable) {
        if (sourceTable == null || targetTable == null) {
            return;
        }

        Table targetTableClone = cloneTable(targetTable);
        targetTable.removeAllColumns(); 

        for (Column sourceColumn : sourceTable.getColumns()) {
            Column targetColumn = targetTableClone.getColumnWithName(sourceColumn.getName());
            if (targetColumn != null) {
                targetTable.addColumn(targetColumn);
            }
        }
    } 

    protected TransformTableNodeGroupLink getTransformFor(Table sourceTable) {
        String sourceNodeGroupId = sourceEngine.getNodeService().findIdentity().getNodeGroupId(); 
        String targetNodeGroupId = targetEngine.getNodeService().findIdentity().getNodeGroupId(); 
        List<TransformTableNodeGroupLink> transforms = 
                sourceEngine.getTransformService().findTransformsFor(
                        sourceNodeGroupId, targetNodeGroupId, sourceTable.getName());
        if (!CollectionUtils.isEmpty(transforms)) {
            TransformTableNodeGroupLink transform = transforms.get(0); // Only can operate on a single table transform for now.
            if (!StringUtils.isEmpty(transform.getFullyQualifiedTargetTableName())) {
                return transform;
            }
        }
        return null;
    }

    protected Table loadTargetTableUsingTransform(Table sourceTable, TransformTableNodeGroupLink transform) {
        Table targetTable = targetEngine.getDatabasePlatform().
                getTableFromCache(transform.getTargetTableName(), true);

        if (targetTable == null) {
            return targetTable;
        }

        if (!CollectionUtils.isEmpty(transform.getTransformColumns())) {
            // attempt to arrange the columns to map the transform.

            Table targetTableClone = cloneTable(targetTable);
            targetTable.removeAllColumns();

            for (Column sourceColumn : sourceTable.getColumns()) {
                List<TransformColumn> sourceTransformColumns = transform.getTransformColumnFor(sourceColumn.getName());
                if (!sourceTransformColumns.isEmpty()) {
                    TransformColumn transformColumn = sourceTransformColumns.get(0);
                    Column targetColumn = targetTableClone.getColumnWithName(transformColumn.getTargetColumnName());
                    targetTable.addColumn(targetColumn);  
                }
            }
        } else {
            alignColumns(sourceTable, targetTable);
        }
        return targetTable;
    }

    protected Table cloneTable(Table table) {
        try {
            return (Table) table.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }        
    }

    protected Map<Table, Table> loadTablesFromArguments() {
        if (CollectionUtils.isEmpty(includedTableNames)) {
            throw new RuntimeException("includedTableNames not provided,  includedTableNames must be provided "
                    + "when not comparing using SymmetricDS config.");
        }

        Map<Table, Table> tablesToCompare = new LinkedHashMap<Table, Table>();

        for (String tableName : includedTableNames) {
            Table sourceTable = new Table(tableName);
            Table destinationTable = new Table(tableName);
            tablesToCompare.put(sourceTable, destinationTable);
        }

        return tablesToCompare;
    }

    protected Map<Table, Table> removeExcludedTables(Map<Table, Table> tablesToCompare) {
        if (CollectionUtils.isEmpty(excludedTableNames)) {
            return tablesToCompare;
        }

        Map<Table, Table> filteredTablesToCompare = new LinkedHashMap<Table, Table>(tablesToCompare);
        for (String excludedTableName : excludedTableNames) {            
            for (Table sourceTable : tablesToCompare.keySet()) {

                if (sourceTable.getName().trim().equalsIgnoreCase(excludedTableName.trim())) {
                    filteredTablesToCompare.remove(sourceTable);
                }
            }
        }

        return filteredTablesToCompare;
    }

    public OutputStream getSqlDiffStream() {
        return sqlDiffStream;
    }

    public void setSqlDiffStream(OutputStream sqlDiffStream) {
        this.sqlDiffStream = sqlDiffStream;
    }

    public List<String> getIncludedTableNames() {
        return includedTableNames;
    }

    public void setIncludedTableNames(List<String> includedTableNames) {
        this.includedTableNames = includedTableNames;
    }

    public List<String> getExcludedTableNames() {
        return excludedTableNames;
    }

    public void setExcludedTableNames(List<String> excludedTableNames) {
        this.excludedTableNames = excludedTableNames;
    }

    public boolean isUseSymmetricConfig() {
        return useSymmetricConfig;
    }

    public void setUseSymmetricConfig(boolean useSymmetricConfig) {
        this.useSymmetricConfig = useSymmetricConfig;
    }

    class CountingSqlReadCursor implements ISqlReadCursor<Row> {

        ISqlReadCursor<Row> wrapped;
        int count = 0;

        CountingSqlReadCursor(ISqlReadCursor<Row> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public Row next() {
            Row row = wrapped.next();
            if (row != null) {
                count++;
            }
            return row;
        }

        @Override
        public void close() {
            wrapped.close();
        }
    }
}
