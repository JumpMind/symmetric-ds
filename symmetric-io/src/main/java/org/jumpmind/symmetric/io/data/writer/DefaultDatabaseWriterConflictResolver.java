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
package org.jumpmind.symmetric.io.data.writer;

import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.TableRow;
import org.jumpmind.exception.ParseException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver extends AbstractDatabaseWriterConflictResolver {

    protected static final Logger log = LoggerFactory.getLogger(DefaultDatabaseWriterConflictResolver.class);

    protected boolean isTimestampNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
        DynamicDefaultDatabaseWriter databaseWriter = (DynamicDefaultDatabaseWriter)writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = databaseWriter.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = databaseWriter.getPlatform().createDmlStatement(DmlType.FROM, targetTable
                , writer.getWriterSettings().getTextColumnExpression());
        Column column = targetTable.getColumnWithName(columnName);
        
        if (column == null) {
            throw new RuntimeException(String.format("Could not find a timestamp column with a name of %s on the table %s.  "
                    + "Please check your conflict resolution configuration", columnName, targetTable.getQualifiedTableName()));
        }
        
        String sql = stmt.getColumnsSql(new Column[] { column });

        Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                CsvData.ROW_DATA);
        String loadingStr = newData.get(columnName);

        Date loadingTs = null;
        Date existingTs = null;
        if (column.isTimestampWithTimezone()) {
            // Get the existingTs with timezone
            String existingStr = databaseWriter.getTransaction().queryForObject(sql, String.class,
                    objectValues);
            // If you are in this situation because of an instance where the conflict exists
            // because the row doesn't exist, then existing simply needs to be null
            if (existingStr != null) {
	            int split = existingStr.lastIndexOf(" ");
	            existingTs = FormatUtils.parseDate(existingStr.substring(0, split).trim(),
	                    FormatUtils.TIMESTAMP_PATTERNS,
	                    TimeZone.getTimeZone(existingStr.substring(split).trim()));
            }
            // Get the loadingTs with timezone
            int split = loadingStr.lastIndexOf(" ");
            loadingTs = FormatUtils.parseDate(loadingStr.substring(0, split).trim(),
                    FormatUtils.TIMESTAMP_PATTERNS,
                    TimeZone.getTimeZone(loadingStr.substring(split).trim()));
        } else {
            // Get the existingTs
            existingTs = databaseWriter.getTransaction().queryForObject(sql, Timestamp.class,
                    objectValues);
            // Get the loadingTs
            Object[] values = platform.getObjectValues(writer.getBatch().getBinaryEncoding(),
                    new String[] { loadingStr }, new Column[] { column });
            if (values[0] instanceof Date) {
                loadingTs = (Date) values[0];
            } else if (values[0] instanceof String &&
                    column.getJdbcTypeName().equalsIgnoreCase(TypeMap.DATETIME2)) {
                // SQL Server DateTime2 type is treated as a string internally.
                loadingTs = databaseWriter.getPlatform().parseDate(Types.VARCHAR, (String)values[0], false);
            } else {
                throw new ParseException("Could not parse " + columnName + " with a value of "
                        + loadingStr + " for purposes of conflict detection");
            }
        }

        return existingTs == null || loadingTs.compareTo(existingTs) > 0;
    }

    protected boolean isVersionNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter)writer;
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = databaseWriter.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = databaseWriter.getPlatform().createDmlStatement(DmlType.FROM, targetTable
                , writer.getWriterSettings().getTextColumnExpression());
        String sql = stmt.getColumnsSql(new Column[] { targetTable.getColumnWithName(columnName) });
        Long existingVersion = null;
        
        try {            
            existingVersion = databaseWriter.getTransaction().queryForObject(sql, Long.class, objectValues);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to execute conflict resolution SQL: \"" + 
                    sql  + "\" values: " + Arrays.toString(objectValues), ex); 
        }
        
        if (existingVersion == null) {
            return true;
        } else {
            Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                    CsvData.ROW_DATA);
            Long loadingVersion = Long.valueOf(newData.get(columnName));
            return loadingVersion > existingVersion;
        }
    }
    
    @Override
    protected boolean checkForUniqueKeyViolation(AbstractDatabaseWriter writer, CsvData data, Conflict conflict, Throwable e) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter)writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        Table targetTable = writer.getTargetTable();
        boolean isPrimaryKeyViolation = false;
        
        if (e != null && sqlTemplate.isUniqueKeyViolation(e)) {
            String violatedIndexName = sqlTemplate.getUniqueKeyViolationIndexName(e);

            if (violatedIndexName != null) {
                // Use the violated unique index name to find the columns involved so we can run a delete statement
                boolean foundUniqueIndex = false;
                int count = 0;
                for (IIndex index : targetTable.getIndices()) {
                    if (index.isUnique() && (index.getName().equals(violatedIndexName) || violatedIndexName.contentEquals("%"))) {
                        foundUniqueIndex = true;
                        log.info("Unique violation from index {} on table {} during {} with batch {}.  Attempting to correct.", violatedIndexName, targetTable.getName(), 
                                data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
                        count += deleteUniqueConstraintRow(platform, sqlTemplate, databaseWriter, targetTable, index, data);
                    }
                }

                if (foundUniqueIndex) {
                    return count != 0;
                } else {
                    // Couldn't find the unique index on the table, so the violation is the internal primary key index
                    isPrimaryKeyViolation = true;
                }
            } else {
                // Couldn't find the unique index name from the exception, so the violation is the internal primary key index
                isPrimaryKeyViolation = true;
            }
        }
        
        if (isPrimaryKeyViolation && data.getDataEventType().equals(DataEventType.UPDATE)) {
            // Primary key is preventing our update, so we delete the blocking row
            Map<String, String> values = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
            List<Column> whereColumns = targetTable.getPrimaryKeyColumnsAsList();
            List<String> whereValues = new ArrayList<String>();
            
            for (Column column : whereColumns) {
                whereValues.add(values.get(column.getName()));
            }            
            return deleteRow(platform, sqlTemplate, databaseWriter, targetTable, whereColumns, whereValues) != 0;
        }
        return false;
    }
 
    protected int deleteUniqueConstraintRow(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable,
            IIndex uniqueIndex, CsvData data) {
        Map<String, String> values = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
        List<Column> whereColumns = new ArrayList<Column>();
        List<String> whereValues = new ArrayList<String>();

        for (IndexColumn indexColumn : uniqueIndex.getColumns()) {
            whereColumns.add(targetTable.getColumnWithName(indexColumn.getName()));
            whereValues.add(values.get(indexColumn.getName()));
        }
        
        return deleteRow(platform, sqlTemplate, databaseWriter, targetTable, whereColumns, whereValues);
    }

    protected int deleteRow(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable,
            List<Column> whereColumns, List<String> whereValues) {
        Object[] objectValues = platform.getObjectValues(databaseWriter.getBatch().getBinaryEncoding(),
                whereValues.toArray(new String[0]), whereColumns.toArray(new Column[0]));
        DmlStatement fromStmt = platform.createDmlStatement(DmlType.FROM, targetTable.getCatalog(), targetTable.getSchema(),
                targetTable.getName(), whereColumns.toArray(new Column[0]), targetTable.getColumns(), null,
                databaseWriter.getWriterSettings().getTextColumnExpression());
        String sql = "DELETE " + fromStmt.getSql();
        int count = 0;
        try {
            count = prepareAndExecute(platform, databaseWriter, sql, objectValues);
            if (count == 0) {
                log.error("Failed to find and delete the blocking row by unique constraint: " + sql + " " + 
                        ArrayUtils.toString(objectValues));
            }
        } catch (SqlException ex) {
            if (sqlTemplate.isForeignKeyChildExistsViolation(ex)) {
                log.info("Child exists foreign key violation while correcting unique constraint violation.  Attempting further corrections.");
                // Failed to delete the row because another row is referencing it                        
                DmlStatement selectStmt = platform.createDmlStatement(DmlType.SELECT, targetTable.getCatalog(), targetTable.getSchema(),
                        targetTable.getName(), whereColumns.toArray(new Column[0]), targetTable.getColumns(), null,
                        databaseWriter.getWriterSettings().getTextColumnExpression());
                // Query the row that we need to delete because it is blocking us                
                Row uniqueRow = queryForRow(platform, databaseWriter, selectStmt.getSql(), objectValues);
                CsvData uniqueData = new CsvData(DataEventType.INSERT, uniqueRow.toStringArray(targetTable.getColumnNames()));
                if (deleteForeignKeyChildren(platform, sqlTemplate, databaseWriter, targetTable, uniqueData)) {
                    count = prepareAndExecute(platform, databaseWriter, sql, objectValues);
                }
            } else {
                throw ex;
            }
        }
        return count;
    }

    @Override
    protected boolean checkForForeignKeyChildExistsViolation(AbstractDatabaseWriter writer, CsvData data, Conflict conflict, Throwable e) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter)writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();

        if (e != null && sqlTemplate.isForeignKeyChildExistsViolation(e)) {
            Table targetTable = writer.getTargetTable();
            log.info("Child exists foreign key violation on table {} during {} with batch {}.  Attempting to correct.", 
                    targetTable.getName(), data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
            
            return deleteForeignKeyChildren(platform, sqlTemplate, databaseWriter, writer.getTargetTable(), data);
        }
        return false;
    }    

    protected boolean deleteForeignKeyChildren(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable, CsvData data) {        
        Map<String, String> values = null;
        if (data.getDataEventType().equals(DataEventType.UPDATE)) {
            values = data.toColumnNameValuePairs(targetTable.getPrimaryKeyColumnNames(), CsvData.PK_DATA);
        } else {
            values = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
        }
        
        List<TableRow> tableRows = new ArrayList<TableRow>();
        tableRows.add(new TableRow(targetTable, values, null, null, null));
        List<TableRow> foreignTableRows = doInTransaction(platform, databaseWriter, new ITransactionCallback<List<TableRow>>() {
            public List<TableRow> execute(ISqlTransaction transaction) {
                return platform.getDdlReader().getExportedForeignTableRows(transaction, tableRows, new HashSet<TableRow>());
            }
        });
        
        if (foreignTableRows.isEmpty()) {
            log.info("Could not determine foreign table rows to fix foreign key violation for table '{}'", targetTable.getFullyQualifiedTableName());
            return false;
        }
        
        Collections.reverse(foreignTableRows);
        Set<TableRow> visited = new HashSet<TableRow>();
        
        for (TableRow foreignTableRow : foreignTableRows) {
            if (visited.add(foreignTableRow)) {
                Table foreignTable = foreignTableRow.getTable();
            
                log.info("Remove foreign row "
                        + "catalog '{}' schema '{}' foreign table name '{}' fk name '{}' where sql '{}' "
                        + "to correct table '{}' for column '{}'",
                        foreignTable.getCatalog(), foreignTable.getSchema(), foreignTable.getName(), foreignTableRow.getFkName(), foreignTableRow.getWhereSql(), 
                        targetTable.getName(), foreignTableRow.getReferenceColumnName());
                
                DatabaseInfo info = platform.getDatabaseInfo();
                String tableName = Table.getFullyQualifiedTableName(foreignTable.getCatalog(), foreignTable.getSchema(), foreignTable.getName(), 
                        info.getDelimiterToken(), info.getCatalogSeparator(), info.getSchemaSeparator());
                String sql = "DELETE FROM " + tableName + " WHERE " + foreignTableRow.getWhereSql();
                prepareAndExecute(platform, databaseWriter, sql);
            }
        }
        return true;
    }

    protected int prepareAndExecute(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, String sql, Object... values) {
        return doInTransaction(platform, databaseWriter, new ITransactionCallback<Integer>() {
            public Integer execute(ISqlTransaction transaction) {
                return transaction.prepareAndExecute(sql, values);
            }
        });
    }
    
    protected Row queryForRow(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, String sql, Object... values) {
        return doInTransaction(platform, databaseWriter, new ITransactionCallback<Row>() {
            public Row execute(ISqlTransaction transaction) {
                return transaction.queryForRow(sql, values);
            }
        });
    }

    private ISqlTransaction getTransaction(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter) {
        // There is code in DefaultDatabaseWriter.insert() that sets up the last error of SQLIntegrityConstraintViolationException
        // when an insert of a record returns a count of 0 records inserted (with no SQL exception).
        // That is so that the conflict resolution code that receives that information can resolve the conflict successfully.
        // As a result of this, part of the conflict resolution is to delete the offending record (the duplicate key record) so
        // that the new data can be inserted successfully.
        // The current transaction needs to be used because there is a possibility that the duplicate record is part of this batch,
        // hence this transaction.
        // The check below is for Postgres and Redshift dialects. When an SQL exception occurs, the transaction will be rolled back
        // for those dialects, so can't use the current transaction. But because an exception is generated (it didn't actually occur,
        // it was created and set to lastError), we need to use the same transaction when it is a special transaction, that is,
        // SQLIntegrityConstraintViolationException.
        if (platform.getDatabaseInfo().isRequiresSavePointsInTransaction() &&
                databaseWriter.getContext().getLastError() != null &&
                ! (databaseWriter.getContext().getLastError() instanceof SQLIntegrityConstraintViolationException))
        {
            return platform.getSqlTemplate().startSqlTransaction(true);
        } else {
            return databaseWriter.getTransaction();
        }
    }
    
    private void doneWithTransaction(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, ISqlTransaction transaction) {
        if (platform.getDatabaseInfo().isRequiresSavePointsInTransaction() &&
                databaseWriter.getContext().getLastError() != null &&
                ! (databaseWriter.getContext().getLastError() instanceof SQLIntegrityConstraintViolationException)) {
            transaction.close();
        }
    }

    private <T> T doInTransaction(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, ITransactionCallback<T> callback) {
        ISqlTransaction transaction = null;
        try {
            transaction = getTransaction(platform, databaseWriter);
            return callback.execute(transaction);
        } finally {
            doneWithTransaction(platform, databaseWriter, transaction);
        }
    }
    
    private interface ITransactionCallback<T> {
        public T execute(ISqlTransaction transaction);
    }

}
