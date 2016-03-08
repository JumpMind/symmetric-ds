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

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.xalan.templates.FuncKey;
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

    private String sqlDiffFileName;
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
        long start = System.currentTimeMillis();
        List<DbCompareTables> tablesToCompare = getTablesToCompare();
        for (DbCompareTables tables : tablesToCompare) {
            TableReport tableReport = compareTables(tables);
            report.addTableReport(tableReport);
            long elapsed = System.currentTimeMillis() - start;
            log.info("Completed table {}.  Elapsed time: {}", tableReport, 
                    DurationFormatUtils.formatDurationWords((elapsed), true, true));
        }

        long totalTime = System.currentTimeMillis() - start;
        log.info("dbcompare complete.  Total Time: {}", 
                DurationFormatUtils.formatDurationWords((totalTime), true, true));

        return report;
    }

    protected TableReport compareTables(DbCompareTables tables) {
        String sourceSelect = getSourceComparisonSQL(tables, sourceEngine.getDatabasePlatform());
        String targetSelect = getTargetComparisonSQL(tables, targetEngine.getDatabasePlatform());

        CountingSqlReadCursor sourceCursor = new CountingSqlReadCursor(sourceEngine.getDatabasePlatform().
                getSqlTemplate().queryForCursor(sourceSelect, defaultRowMapper));
        CountingSqlReadCursor targetCursor = new CountingSqlReadCursor(targetEngine.getDatabasePlatform().
                getSqlTemplate().queryForCursor(targetSelect, defaultRowMapper));

        TableReport tableReport = new TableReport();
        tableReport.setSourceTable(tables.getSourceTable().getFullyQualifiedTableName());
        tableReport.setTargetTable(tables.getTargetTable().getFullyQualifiedTableName());

        Row sourceRow = sourceCursor.next();
        Row targetRow = targetCursor.next();
        

        int counter = 0;
        long startTime = System.currentTimeMillis();
        DbCompareDiffWriter diffWriter = new DbCompareDiffWriter(targetEngine, tables, sqlDiffFileName);

        try {        
            while (true) {  
                if (sourceRow == null && targetRow == null) {
                    break;
                }
    
                counter++;
                if ((counter % 50000) == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    log.info("{} rows processed for table {}. Elapsed time {}. ({} ms.) Current report status {}", 
                            counter, tables.getSourceTable().getName(), 
                            DurationFormatUtils.formatDurationWords((elapsed), true, true), elapsed,
                            tableReport);
                }
    
                DbCompareRow sourceCompareRow = sourceRow != null ? 
                        new DbCompareRow(sourceEngine, dbValueComparator, tables.getSourceTable(), sourceRow) : null;
                DbCompareRow targetCompareRow = targetRow != null ? 
                        new DbCompareRow(targetEngine, dbValueComparator,  tables.getTargetTable(), targetRow) : null;
    
                int comparePk = comparePk(tables, sourceCompareRow, targetCompareRow);
                if (comparePk == 0) {
                    Map<Column, String> deltas = sourceCompareRow.compareTo(tables, targetCompareRow);
                    if (deltas.isEmpty()) {
                        tableReport.countMatchedRow();                    
                    } else {
                        diffWriter.writeUpdate(targetCompareRow, deltas);
                        tableReport.countDifferentRow();
                    }
    
                    sourceRow = sourceCursor.next();
                    targetRow = targetCursor.next();
                } else if (comparePk < 0) {
                    diffWriter.writeInsert(sourceCompareRow);
                    tableReport.countMissingRow();
                    sourceRow = sourceCursor.next();
                } else {
                    diffWriter.writeDelete(targetCompareRow);
                    tableReport.countExtraRow();
                    targetRow = targetCursor.next();
                }
                tableReport.setSourceRows(sourceCursor.count);
                tableReport.setTargetRows(targetCursor.count);
            }
        } finally {
            diffWriter.close();
        }

        return tableReport;
    }

    protected int comparePk(DbCompareTables tables, DbCompareRow sourceCompareRow, DbCompareRow targetCompareRow) {
        if (sourceCompareRow != null && targetCompareRow == null) {
            return -1;
        }
        if (sourceCompareRow == null && targetCompareRow != null) {
            return 1;
        }

        return sourceCompareRow.comparePks(tables, targetCompareRow);
    }

    protected String getSourceComparisonSQL(DbCompareTables tables, IDatabasePlatform platform) {
        return getComparisonSQL(tables.getSourceTable(),
                tables.getSourceTable().getPrimaryKeyColumns(), platform);
    }
    
    protected String getTargetComparisonSQL(DbCompareTables tables, IDatabasePlatform platform) {
        List<Column> mappedPkColumns = new ArrayList<Column>();
        
        for (Column sourcePkColumn : tables.getSourceTable().getPrimaryKeyColumns()) {
            Column targetColumn = tables.getColumnMapping().get(sourcePkColumn);
            if (targetColumn == null) {
                log.warn("No target column mapped to source PK column {}.  Dbcompare may be inaccurate for this table.", sourcePkColumn);
            } else {
                mappedPkColumns.add(targetColumn);
            }
        }
        
        return getComparisonSQL(tables.getTargetTable(), mappedPkColumns.toArray(new Column[0]), platform);
    }
    
    protected String getComparisonSQL(Table table, Column[] sortByColumns, IDatabasePlatform platform) {
        DmlStatement statement = platform.createDmlStatement(DmlType.SELECT,
                table.getCatalog(), table.getSchema(), table.getName(),
                null, table.getColumns(),
                null, null);

        StringBuilder sql = new StringBuilder(statement.getSql());
        sql.append("1=1 ");

        sql.append(buildOrderBy(table, sortByColumns, platform));
        log.info("Comparison SQL: {}", sql);
        return sql.toString();
    }

    protected String buildOrderBy(Table table, Column[] sortByColumns, IDatabasePlatform platform) {
        DatabaseInfo databaseInfo = platform.getDatabaseInfo();
        String quote = databaseInfo.getDelimiterToken() == null ? "" : databaseInfo.getDelimiterToken(); 
        StringBuilder orderByClause = new StringBuilder("ORDER BY ");
        for (Column sortByColumn : sortByColumns) {
            String columnName = new StringBuilder(quote).append(sortByColumn.getName()).append(quote).toString();
            orderByClause.append(columnName);
            orderByClause.append(",");
        }
        orderByClause.setLength(orderByClause.length()-1);
        return orderByClause.toString();       
    }

    protected List<DbCompareTables> getTablesToCompare() {
        List<DbCompareTables> tablesToCompare;
        if (useSymmetricConfig) {
            tablesToCompare = loadTablesFromConfig();
        } else {
            tablesToCompare = loadTablesFromArguments();
        }

        return tablesToCompare;
    }


    protected  List<DbCompareTables> loadTablesFromConfig() {        
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

    protected List<DbCompareTables> loadTables(List<String> tableNames) {

        List<DbCompareTables> tablesFromConfig = new ArrayList<DbCompareTables>(1);

        List<String> filteredTablesNames = filterTables(tableNames);

        for (String tableName : filteredTablesNames) {
            Table sourceTable = null;
            Map<String, String> tableNameParts = sourceEngine.getDatabasePlatform().parseQualifiedTableName(tableName);
            if (tableNameParts.size() == 1) {
                sourceTable = sourceEngine.getDatabasePlatform().getTableFromCache(tableName, true);
            } else {
                sourceTable = sourceEngine.getDatabasePlatform().
                        getTableFromCache(tableNameParts.get("catalog"), tableNameParts.get("schema"), tableNameParts.get("table"), true);
            }

            if (sourceTable == null) {
                log.warn("No source table found for table name {}", tableName);
                continue;
            }
            if (sourceTable.getPrimaryKeyColumnCount() == 0) {
                log.warn("Source table {} doesn't have any primary key columns and will not be considered in the comparison.", sourceTable);
                continue;                
            }

            DbCompareTables tables = new DbCompareTables(sourceTable, null);

            Table targetTable = loadTargetTable(tables);
            if (targetTable == null) {
                log.warn("No target table found for table {}", tableName);
                continue;
            } 
//            else if (targetTable.getPrimaryKeyColumnCount() == 0) {
//                log.warn("Target table {} doesn't have any primary key columns and will not be considered in the comparison.", targetTable);
//                continue;                
//            }          

            tables.applyColumnMappings();
            tablesFromConfig.add(tables);
        }

        return tablesFromConfig;        
    }

    protected Table loadTargetTable(DbCompareTables tables) {
        Table targetTable = null;
        if (useSymmetricConfig) {
            TransformTableNodeGroupLink transform = getTransformFor(tables.getSourceTable());
            if (transform != null) {
                targetTable =  loadTargetTableUsingTransform(transform);
                tables.setTargetTable(targetTable);
                tables.setTransform(transform);
                return targetTable;
            }
        } 

        targetTable = targetEngine.getDatabasePlatform().
                getTableFromCache(tables.getSourceTable().getName(), true);
        tables.setTargetTable(targetTable);

        return targetTable;
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

    protected Table loadTargetTableUsingTransform(TransformTableNodeGroupLink transform) {
        Table targetTable = targetEngine.getDatabasePlatform().	
                getTableFromCache(transform.getTargetCatalogName(), transform.getTargetSchemaName(), transform.getTargetTableName(), true); 

        return targetTable;
    }

    protected Table cloneTable(Table table) {
        try {
            return (Table) table.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }        
    }

    protected List<DbCompareTables> loadTablesFromArguments() {
        if (CollectionUtils.isEmpty(includedTableNames)) {
            throw new RuntimeException("includedTableNames not provided,  includedTableNames must be provided "
                    + "when not comparing using SymmetricDS config.");
        }

        return loadTables(includedTableNames);
    }

    protected List<String> filterTables(List<String> tables) {        
        List<String> filteredTables = new ArrayList<String>(tables.size());

        if (!CollectionUtils.isEmpty(includedTableNames)) {
            for (String includedTableName : includedTableNames) {                
                for (String tableName : tables) {
                    if (StringUtils.equalsIgnoreCase(tableName.trim(), includedTableName.trim())) {
                        filteredTables.add(tableName);
                    }
                }
            }
        } else {
            filteredTables.addAll(tables);
        }

        if (!CollectionUtils.isEmpty(excludedTableNames)) {
            List<String> excludedTables = new ArrayList<String>(filteredTables);

            for (String excludedTableName : excludedTableNames) {            
                for (String tableName : filteredTables) {
                    if (StringUtils.equalsIgnoreCase(tableName.trim(), excludedTableName.trim())) {
                        excludedTables.remove(tableName);
                    }
                }
            }
            return excludedTables;
        }        

        return filteredTables;
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

    public String getSqlDiffFileName() {
        return sqlDiffFileName;
    }

    public void setSqlDiffFileName(String sqlDiffFileName) {
        this.sqlDiffFileName = sqlDiffFileName;
    }
}
