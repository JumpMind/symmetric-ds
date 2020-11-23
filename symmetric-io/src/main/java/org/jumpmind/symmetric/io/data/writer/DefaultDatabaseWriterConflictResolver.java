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

import org.apache.commons.lang3.ArrayUtils;
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
import org.jumpmind.db.util.DatabaseConstants;
import org.jumpmind.db.util.TableRow;
import org.jumpmind.exception.ParseException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
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
            String existingStr = databaseWriter.getTransaction().queryForObject(sql, String.class,
                    objectValues);

            if (existingStr != null) {
                existingTs = FormatUtils.parseTimestampWithTimezone(existingStr, FormatUtils.TIMESTAMP_WITH_TIMEZONE_PATTERNS);
            }

            if (loadingStr != null) {
                loadingTs = FormatUtils.parseTimestampWithTimezone(loadingStr, FormatUtils.TIMESTAMP_WITH_TIMEZONE_PATTERNS);
            }
        } else {
            existingTs = databaseWriter.getTransaction().queryForObject(sql, Timestamp.class,
                    objectValues);

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

        return existingTs == null || (loadingTs != null && loadingTs.compareTo(existingTs) > 0);
    }

    protected boolean isCaptureTimeNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
        DynamicDefaultDatabaseWriter databaseWriter = (DynamicDefaultDatabaseWriter) writer;
        Table targetTable = writer.getTargetTable();
        String[] pkData = data.getPkData(targetTable);
        String pkCsv = CsvUtils.escapeCsvData(pkData);
        Timestamp loadingTs = data.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME);
        Date existingTs = null;
        String existingNodeId = null;
        boolean isLoadOnlyNode = databaseWriter.getWriterSettings().isLoadOnlyNode();

        if (loadingTs != null && !isLoadOnlyNode) {
            if (log.isDebugEnabled()) {
                log.debug("Finding last capture time for table {} with pk of {}", targetTable.getName(), ArrayUtils.toString(pkData));
            }

            if (databaseWriter.getPlatform(targetTable.getName()).supportsMultiThreadedTransactions()) {
                // make sure we lock the row that is in conflict to prevent a race with other data loading
                DmlStatement st = databaseWriter.getPlatform().createDmlStatement(DmlType.UPDATE, targetTable.getCatalog(), targetTable.getSchema(), 
                        targetTable.getName(), targetTable.getPrimaryKeyColumns(), targetTable.getPrimaryKeyColumns(), 
                        new boolean[targetTable.getPrimaryKeyColumnCount()], databaseWriter.getWriterSettings().getTextColumnExpression());
                Object[] values = databaseWriter.getPlatform().getObjectValues(writer.getBatch().getBinaryEncoding(), 
                        pkData, targetTable.getPrimaryKeyColumns());
                databaseWriter.getTransaction().prepareAndExecute(st.getSql(), ArrayUtils.addAll(values, values));
            }

            String sql = "select source_node_id, create_time from " + databaseWriter.getTablePrefix() + 
                    "_data where table_name = ? and ((event_type = 'I' and row_data like ?) or " +
                    "(event_type in ('U', 'D') and pk_data like ?)) and create_time >= ? order by create_time desc";
            
            Object[] args = new Object[] { targetTable.getName(), pkCsv + "%", pkCsv, loadingTs };
            Row row = null;

            if (databaseWriter.getPlatform(targetTable.getName()).supportsMultiThreadedTransactions()) {
                // we may have waited for another transaction to commit, so query with a new transaction
                row = databaseWriter.getPlatform(targetTable.getName()).getSqlTemplateDirty().queryForRow(sql, args);
            } else {
                row = writer.getContext().findTransaction().queryForRow(sql, args);
            }

            if (row != null) {
                existingTs = row.getDateTime("create_time");
                existingNodeId = row.getString("source_node_id");
                if (existingNodeId == null || existingNodeId.equals("")) {
                    existingNodeId = writer.getContext().getBatch().getTargetNodeId();
                }
            }
        }

        boolean isWinner = isLoadOnlyNode || existingTs == null || (loadingTs != null && (loadingTs.getTime() > existingTs.getTime() 
                || (loadingTs.getTime() == existingTs.getTime() && writer.getContext().getBatch().getSourceNodeId().hashCode() > existingNodeId.hashCode())));
        writer.getContext().put(DatabaseConstants.IS_CONFLICT_WINNER, isWinner);
        
        if (log.isDebugEnabled()) {
            log.debug("{} row from batch {} with local time of {} and remote time of {} for table {} and pk of {}",
                    isWinner ? "Winning" : "Losing", writer.getContext().getBatch().getNodeBatchId(), 
                            existingTs, loadingTs, targetTable.getName(), ArrayUtils.toString(pkData));
        }
        return isWinner;
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
        int count = 0;
        
        if (e != null && sqlTemplate.isUniqueKeyViolation(e)) {
            Table targetTable = writer.getTargetTable();
            log.info("Unique key violation on table {} during {} with batch {}.  Attempting to correct.", 
                    targetTable.getName(), data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());

            for (IIndex index : targetTable.getIndices()) {
                if (index.isUnique()) {
                    log.info("Correcting for possible violation of unique index {} on table {} during {} with batch {}", index.getName(), 
                            targetTable.getName(), data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
                    count += deleteUniqueConstraintRow(platform, sqlTemplate, databaseWriter, targetTable, index, data);
                }
            }

            if (data.getDataEventType().equals(DataEventType.UPDATE)) {
                // Primary key is preventing our update, so we delete the blocking row
                log.info("Correcting for possible violation of primary key on table {} during {} with batch {}", targetTable.getName(), 
                        data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
    
                Map<String, String> values = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                List<Column> whereColumns = targetTable.getPrimaryKeyColumnsAsList();
                List<String> whereValues = new ArrayList<String>();
                
                for (Column column : whereColumns) {
                    whereValues.add(values.get(column.getName()));
                }            
                count += deleteRow(platform, sqlTemplate, databaseWriter, targetTable, whereColumns, whereValues, false);
            }
        }
        return count != 0;
    }
 
    protected int deleteUniqueConstraintRow(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable,
            IIndex uniqueIndex, CsvData data) {
        Map<String, String> values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getColumnNames(), CsvData.ROW_DATA);
        List<Column> whereColumns = new ArrayList<Column>();
        List<String> whereValues = new ArrayList<String>();

        for (IndexColumn indexColumn : uniqueIndex.getColumns()) {
            whereColumns.add(targetTable.getColumnWithName(indexColumn.getName()));
            whereValues.add(values.get(indexColumn.getName()));
        }
        
        return deleteRow(platform, sqlTemplate, databaseWriter, targetTable, whereColumns, whereValues, true);
    }

    protected int deleteRow(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable,
            List<Column> whereColumns, List<String> whereValues, boolean isUniqueKey) {
        Object[] values = platform.getObjectValues(databaseWriter.getBatch().getBinaryEncoding(),
                whereValues.toArray(new String[0]), whereColumns.toArray(new Column[0]));
        boolean[] nullKeyValues = new boolean[values.length];
        List<Object> valuesList = new ArrayList<Object>();
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                nullKeyValues[i] = true;
            } else {
                valuesList.add(values[i]);
            } 
        }
        Object[] objectValues = valuesList.toArray(new Object[0]);
        DmlStatement fromStmt = platform.createDmlStatement(DmlType.FROM, targetTable.getCatalog(), targetTable.getSchema(),
                targetTable.getName(), whereColumns.toArray(new Column[0]), targetTable.getColumns(), nullKeyValues,
                databaseWriter.getWriterSettings().getTextColumnExpression());
        String sql = "DELETE " + fromStmt.getSql();
        int count = 0;
        try {
            count = prepareAndExecute(platform, databaseWriter, sql, objectValues);
            if (count == 0) {
                log.error("Failed to find and delete the blocking row by {}: {} {}",
                        isUniqueKey ? "unique constraint" : "primary key", sql, ArrayUtils.toString(objectValues));
            }
        } catch (SqlException ex) {
            if (sqlTemplate.isForeignKeyChildExistsViolation(ex)) {
                log.info("Child exists foreign key violation while correcting {} violation.  Attempting further corrections.",
                        isUniqueKey ? "unique constraint" : "primary key");
                // Failed to delete the row because another row is referencing it                        
                DmlStatement selectStmt = platform.createDmlStatement(DmlType.SELECT, targetTable.getCatalog(), targetTable.getSchema(),
                        targetTable.getName(), whereColumns.toArray(new Column[0]), targetTable.getColumns(), nullKeyValues,
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
        if (data.getDataEventType().equals(DataEventType.INSERT)) {
            values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getColumnNames(), CsvData.ROW_DATA);
        } else {
            values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getColumnNames(), CsvData.OLD_DATA);
            if (values == null || values.size() == 0) {
                values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getPrimaryKeyColumnNames(), CsvData.PK_DATA);
            }
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
