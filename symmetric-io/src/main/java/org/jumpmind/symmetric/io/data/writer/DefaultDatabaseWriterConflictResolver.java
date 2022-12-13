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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Reference;
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
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.db.util.BinaryEncoding;
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

    @Override
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

    @Override
    protected boolean isCaptureTimeNewer(Conflict conflict, AbstractDatabaseWriter writer, CsvData data) {
        DynamicDefaultDatabaseWriter databaseWriter = (DynamicDefaultDatabaseWriter) writer;
        Table targetTable = writer.getTargetTable();
        String[] pkData = data.getPkData(targetTable);
        String pkCsv = CsvUtils.escapeCsvData(pkData);
        Timestamp loadingTs = data.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME);
        Date existingTs = null;
        String existingNodeId = null;
        boolean isLoadOnlyNode = databaseWriter.getWriterSettings().isLoadOnlyNode();
        boolean isWinnerByUk = true;

        if (loadingTs != null && !isLoadOnlyNode) {
            if (log.isDebugEnabled()) {
                log.debug("Finding last capture time for table {} with pk of {}", targetTable.getName(), ArrayUtils.toString(pkData));
            }

            if (databaseWriter.getPlatform(targetTable.getName()).supportsMultiThreadedTransactions() &&
                    (!databaseWriter.getPlatform().getDatabaseInfo().isRequiresSavePointsInTransaction() ||
                            !Boolean.TRUE.equals(databaseWriter.getContext().get(AbstractDatabaseWriter.TRANSACTION_ABORTED)))) {
                // make sure we lock the row that is in conflict to prevent a race with other data loading
                if (primaryKeyUpdateAllowed(databaseWriter, targetTable)) {
                    DmlStatement st = databaseWriter.getPlatform().createDmlStatement(DmlType.UPDATE, targetTable.getCatalog(), targetTable.getSchema(), 
                            targetTable.getName(), targetTable.getPrimaryKeyColumns(), targetTable.getPrimaryKeyColumns(), 
                            new boolean[targetTable.getPrimaryKeyColumnCount()], databaseWriter.getWriterSettings().getTextColumnExpression());
                    Object[] values = databaseWriter.getPlatform().getObjectValues(writer.getBatch().getBinaryEncoding(), 
                            pkData, targetTable.getPrimaryKeyColumns());
                    databaseWriter.getTransaction().prepareAndExecute(st.getSql(), ArrayUtils.addAll(values, values), st.getTypes());
                } else {
                    Column[] columns = targetTable.getNonPrimaryKeyColumns();
                    if (columns != null && columns.length > 0) {
                        Map<String, String> rowDataMap = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                        DmlStatement st = databaseWriter.getPlatform().createDmlStatement(DmlType.UPDATE, targetTable.getCatalog(), targetTable.getSchema(), 
                                targetTable.getName(), targetTable.getPrimaryKeyColumns(), new Column[] { columns[0] }, 
                                new boolean[targetTable.getPrimaryKeyColumnCount()], databaseWriter.getWriterSettings().getTextColumnExpression());
                        Object[] values = databaseWriter.getPlatform().getObjectValues(writer.getBatch().getBinaryEncoding(), 
                                ArrayUtils.addAll(new String[] { rowDataMap.get(columns[0].getName()) }, pkData),
                                ArrayUtils.addAll(new Column[] { columns[0] }, targetTable.getPrimaryKeyColumns()));
                        databaseWriter.getTransaction().prepareAndExecute(st.getSql(), values,  st.getTypes());
                    }
                }
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

            if (!data.getDataEventType().equals(DataEventType.DELETE)) {
                isWinnerByUk = isCaptureTimeNewerForUk(writer, data);
            }
        }

        boolean isWinner = isLoadOnlyNode || (existingTs == null && isWinnerByUk) || (isWinnerByUk && loadingTs != null && (loadingTs.getTime() > existingTs.getTime() 
                || (loadingTs.getTime() == existingTs.getTime() && writer.getContext().getBatch().getSourceNodeId().hashCode() > existingNodeId.hashCode())));
        if (!isLoadOnlyNode) {
            writer.getContext().put(DatabaseConstants.IS_CONFLICT_WINNER, isWinner);
        }
        if (!isWinner && !isWinnerByUk) {
            Set<String> conflictLosingParentRows = writer.getWriterSettings().getConflictLosingParentRows();
            if (conflictLosingParentRows != null) {
                Map<String, String> rowDataMap = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                conflictLosingParentRows.add(getConflictRowKey(targetTable, rowDataMap));
            }
        }
        
        if (log.isDebugEnabled()) {
            log.debug("{} row from batch {} with local time of {} and remote time of {} for table {} and pk of {}",
                    isWinner ? "Winning" : "Losing", writer.getContext().getBatch().getNodeBatchId(), 
                            existingTs, loadingTs, targetTable.getName(), ArrayUtils.toString(pkData));
        }
        return isWinner;
    }
    
    protected boolean primaryKeyUpdateAllowed(DynamicDefaultDatabaseWriter databaseWriter, Table targetTable) {
        if (!databaseWriter.getPlatform(targetTable.getName()).getDatabaseInfo().isAutoIncrementUpdateAllowed()) {
            for (Column column : targetTable.getPrimaryKeyColumns()) {
                if (column.isAutoIncrement()) {
                    return false;
                }
            }
        }
        return true;
    }
    
    protected boolean uniqueKeyUpdateAllowed(DynamicDefaultDatabaseWriter databaseWriter, Table targetTable, Column[] uniqueColumns) {
        if (!databaseWriter.getPlatform(targetTable.getName()).getDatabaseInfo().isAutoIncrementUpdateAllowed()) {
            for (Column column : uniqueColumns) {
                if (column.isAutoIncrement()) {
                    return false;
                }
            }
        }
        return true;
    }

    protected boolean isCaptureTimeNewerForUk(AbstractDatabaseWriter writer, CsvData data) {
        DynamicDefaultDatabaseWriter databaseWriter = (DynamicDefaultDatabaseWriter) writer;
        Table targetTable = writer.getTargetTable();
        String[] ukData = new String[0];
        Timestamp loadingTs = data.getAttribute(CsvData.ATTRIBUTE_CREATE_TIME);
        Date existingTs = null;
        String existingNodeId = null;
        boolean isWinner = true;

        for (IIndex index : targetTable.getIndices()) {
            if (index.isUnique()) {
                Map<String, String> rowDataMap = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
                Column[] uniqueKeyColumns = new Column[index.getColumnCount()];
                ukData = new String[index.getColumnCount()];
                boolean[] nullKeyValues = new boolean[index.getColumnCount()];
                StringBuilder ukCsv = new StringBuilder();
                int i = 0;
                for (Column column : targetTable.getColumns()) {
                    if (ukCsv.length() > 0) {
                        ukCsv.append(",");
                    }
                    if (index.hasColumn(column)) {
                        uniqueKeyColumns[i] = column;
                        ukData[i] = rowDataMap.get(column.getName());
                        nullKeyValues[i] = ukData[i] == null;
                        ukCsv.append(CsvUtils.escapeCsvData(new String[] { ukData[i++] }));
                    } else {
                        ukCsv.append("%");
                    }
                }

                int count = 0;
                Object[] values = databaseWriter.getPlatform().getObjectValues(writer.getBatch().getBinaryEncoding(), ukData, uniqueKeyColumns);
                if (values == null || values.length == 0) {
                    break;
                }
                if (!databaseWriter.getPlatform(targetTable.getName()).supportsMultiThreadedTransactions() ||
                        (databaseWriter.getPlatform().getDatabaseInfo().isRequiresSavePointsInTransaction() && 
                        Boolean.TRUE.equals(databaseWriter.getContext().get(AbstractDatabaseWriter.TRANSACTION_ABORTED)))) {
                    DmlStatement st = databaseWriter.getPlatform().createDmlStatement(DmlType.COUNT, targetTable.getCatalog(), targetTable.getSchema(), 
                            targetTable.getName(), uniqueKeyColumns, uniqueKeyColumns, nullKeyValues, 
                            databaseWriter.getWriterSettings().getTextColumnExpression());
                    count = databaseWriter.getPlatform(targetTable.getName()).getSqlTemplateDirty().queryForInt(st.getSql(), addKeyArgs(null, values));                    
                } else if (uniqueKeyUpdateAllowed(databaseWriter, targetTable, uniqueKeyColumns)) {
                    // make sure we lock the row that is in conflict to prevent a race with other data loading
                    DmlStatement st = databaseWriter.getPlatform().createDmlStatement(DmlType.UPDATE, targetTable.getCatalog(), targetTable.getSchema(), 
                            targetTable.getName(), uniqueKeyColumns, uniqueKeyColumns, nullKeyValues, 
                            databaseWriter.getWriterSettings().getTextColumnExpression());
                    count = databaseWriter.getTransaction().prepareAndExecute(st.getSql(), addKeyArgs(values, values));                    
                }

                if (count > 0) {
                    String sql = "select source_node_id, create_time from " + databaseWriter.getTablePrefix() + 
                            "_data where table_name = ? and event_type in ('I', 'U') and row_data like ? and " +
                            "create_time >= ? order by create_time desc";
                    Object[] args = new Object[] { targetTable.getName(), ukCsv.toString(), loadingTs };
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
                
                isWinner = existingTs == null || (loadingTs != null && (loadingTs.getTime() > existingTs.getTime() 
                        || (loadingTs.getTime() == existingTs.getTime() && writer.getContext().getBatch().getSourceNodeId().hashCode() > existingNodeId.hashCode())));

                if (log.isDebugEnabled()) {
                    log.debug("{} row from batch {} with local time of {} and remote time of {} for table {} and uk of {}",
                            isWinner ? "Winning" : "Losing", writer.getContext().getBatch().getNodeBatchId(), 
                                    existingTs, loadingTs, targetTable.getName(), ArrayUtils.toString(ukData));
                }
                break;
            }
        }
        return isWinner;
    }

    protected Object[] addKeyArgs(Object[] currentArgs, Object[] additionalArgs) {
        Object[] args = currentArgs;
        if (additionalArgs != null) {
            for (Object arg : additionalArgs) {
                if (arg != null) {
                    args = ArrayUtils.addAll(args, arg);                                
                }
            }
        }
        return args;
    }

    @Override
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
    protected boolean checkForUniqueKeyViolation(AbstractDatabaseWriter writer, CsvData data, Conflict conflict, Throwable e, boolean isFallback) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter)writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        int count = 0;
        
        if (e != null && sqlTemplate.isUniqueKeyViolation(e)) {
            Table targetTable = writer.getTargetTable();
            Map<String, String> values = data.toColumnNameValuePairs(writer.getSourceTable().getColumnNames(), CsvData.ROW_DATA);
            List<Column> whereColumns = targetTable.getPrimaryKeyColumnsAsList();
            List<String> whereValues = new ArrayList<String>();
            for (Column column : whereColumns) {
                whereValues.add(values.get(column.getName()));
            }
            boolean[] nullKeyValues = new boolean[whereColumns.size()];
            DmlStatement countStmt = platform.createDmlStatement(DmlType.COUNT, targetTable.getCatalog(), targetTable.getSchema(),
                    targetTable.getName(), whereColumns.toArray(new Column[0]), targetTable.getPrimaryKeyColumns(), nullKeyValues,
                    databaseWriter.getWriterSettings().getTextColumnExpression());
            Object[] objectValues = platform.getObjectValues(databaseWriter.getBatch().getBinaryEncoding(),
                    whereValues.toArray(new String[0]), whereColumns.toArray(new Column[0]));
            int pkCount = queryForInt(platform, databaseWriter, countStmt.getSql(), objectValues, countStmt.getTypes());
            boolean isUniqueKeyBlocking = false;
            boolean isPrimaryKeyBlocking = false;

            if ((!isFallback && data.getDataEventType().equals(DataEventType.UPDATE)) || 
                    (isFallback && data.getDataEventType().equals(DataEventType.INSERT))) {
                Map<String, String> pkValues = data.toColumnNameValuePairs(writer.getSourceTable().getPrimaryKeyColumnNames(), CsvData.PK_DATA);
                boolean isPkChanged = false;
                if (pkValues.size() > 0) {
                    for (String name : targetTable.getPrimaryKeyColumnNames()) {
                        if (!StringUtils.equals(values.get(name), pkValues.get(name))) {
                            isPkChanged = true;
                            break;
                        }
                    }
                }
                if (isPkChanged && pkCount > 0) {
                    isPrimaryKeyBlocking = true;
                } else {
                    isUniqueKeyBlocking = true;
                }
            } else if (isFallback && data.getDataEventType().equals(DataEventType.UPDATE)) {
                if (pkCount > 0) {
                    isPrimaryKeyBlocking = true;
                } else {
                    isUniqueKeyBlocking = true;
                }
            } else if (!isFallback && data.getDataEventType().equals(DataEventType.INSERT) && pkCount == 0) {
                isUniqueKeyBlocking = true;
            }

            if (isUniqueKeyBlocking) {
                log.info("Unique key violation on table {} during {} with batch {}.  Attempting to correct.", 
                        targetTable.getName(), data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
    
                for (IIndex index : targetTable.getIndices()) {
                    if (index.isUnique()) {
                        log.info("Correcting for possible violation of unique index {} on table {} during {} with batch {}", index.getName(), 
                                targetTable.getName(), data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
                        count += deleteUniqueConstraintRow(platform, sqlTemplate, databaseWriter, targetTable, index, data);
                    }
                }
            } else if (isPrimaryKeyBlocking) {
                log.info("Correcting for update violation of primary key on table {} during {} with batch {}", targetTable.getName(), 
                        data.getDataEventType().toString(), writer.getContext().getBatch().getNodeBatchId());
                count += deleteRow(platform, sqlTemplate, databaseWriter, targetTable, whereColumns, whereValues, false);                            
            }
        }
        return count != 0;
    }
 
    @Override
    protected boolean isConflictingLosingParentRow(AbstractDatabaseWriter writer, CsvData data) {
        DefaultDatabaseWriter databaseWriter = (DefaultDatabaseWriter) writer;
        IDatabasePlatform platform = databaseWriter.getPlatform();
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        Set<String> conflictLosingParentRows = writer.getWriterSettings().getConflictLosingParentRows();
        

        if (conflictLosingParentRows != null && conflictLosingParentRows.size() > 0 && !writer.getBatch().isInitialLoad() &&
                (platform.getDatabaseInfo().isRequiresSavePointsInTransaction() || (writer.getContext().getLastError() != null && 
                sqlTemplate.isForeignKeyViolation(writer.getContext().getLastError())))) {
            Table targetTable = writer.getTargetTable();
            Map<String, String> values = data.toColumnNameValuePairs(targetTable.getColumnNames(), CsvData.ROW_DATA);
            for (ForeignKey fk : targetTable.getForeignKeys()) {
                StringBuilder parentRowKey = new StringBuilder();
                parentRowKey.append(fk.getForeignTableName()).append(":");
                int i = 0;
                for (Reference ref : fk.getReferences()) {
                    if (i++ > 0) {
                        parentRowKey.append(":");
                    }
                    parentRowKey.append(values.get(ref.getLocalColumnName()));
                }
                if (conflictLosingParentRows.contains(parentRowKey.toString())) {
                    conflictLosingParentRows.add(getConflictRowKey(targetTable, values));
                    log.info("Detected losing row for batch {} for missing foreign key parent {}", writer.getBatch().getNodeBatchId(), parentRowKey);
                    return true;
                }
            }
        }
        return false;
    }

    protected String getConflictRowKey(Table table, Map<String, String> values) {
        StringBuilder rowKey = new StringBuilder();
        rowKey.append(table.getName()).append(":");
        int j = 0;
        for (Column pk : table.getPrimaryKeyColumns()) {
            if (j++ > 0) {
                rowKey.append(":");
            }
            rowKey.append(values.get(pk.getName()));
        }
        return rowKey.toString();
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
                log.info("Could not find and delete the blocking row by {}: {} {}",
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
            
            if (deleteForeignKeyChildren(platform, sqlTemplate, databaseWriter, writer.getTargetTable(), data)) {
                return true;
            } else {
                throw new RuntimeException("Failed to delete foreign table rows to fix foreign key violation for table '"
                        + writer.getTargetTable().getFullyQualifiedTableName() + "'");
            }
        }
        return false;
    }    

    protected boolean deleteForeignKeyChildren(IDatabasePlatform platform, ISqlTemplate sqlTemplate, DefaultDatabaseWriter databaseWriter, Table targetTable, CsvData data) {        
        Map<String, String> values = null;
        List<TableRow> tableRows = new ArrayList<TableRow>();
        if (data.getDataEventType().equals(DataEventType.INSERT)) {
            values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getColumnNames(), CsvData.ROW_DATA);
        } else {
            values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getColumnNames(), CsvData.OLD_DATA);
            if (values == null || values.size() == 0) {
                values = data.toColumnNameValuePairs(databaseWriter.getSourceTable().getPrimaryKeyColumnNames(), CsvData.PK_DATA);
                Row whereRow = new Row(values.size());
                boolean[] nullValues = new boolean[values.size()];
                int index = 0;
                for (Entry<String, String> entry : values.entrySet()) {
                    nullValues[index++] = entry.getValue() == null;
                    whereRow.put(entry.getKey(), entry.getValue());
                }
                DmlStatement whereSt = platform.createDmlStatement(DmlType.WHERE, targetTable.getCatalog(),
                        targetTable.getSchema(), targetTable.getName(), targetTable.getPrimaryKeyColumns(),
                        targetTable.getColumns(), nullValues, null);
                String whereSql = whereSt.buildDynamicSql(BinaryEncoding.HEX, whereRow, false, true,
                        targetTable.getPrimaryKeyColumns()).substring(6);
                String delimiter = platform.getDatabaseInfo().getSqlCommandDelimiter();
                if (delimiter != null && delimiter.length() > 0) {
                    whereSql = whereSql.substring(0, whereSql.length() - delimiter.length());
                }
                DmlStatement selectSt = platform.createDmlStatement(DmlType.SELECT, targetTable, null);
                Object[] keys = whereRow.toArray(targetTable.getPrimaryKeyColumnNames());
                Row targetRow = doInTransaction(platform, databaseWriter, new ITransactionCallback<Row>() {
                    public Row execute(ISqlTransaction transaction) {
                        return transaction.queryForRow(selectSt.getSql(), keys);
                    }
                });
                if (targetRow != null) {
                    tableRows.add(new TableRow(targetTable, targetRow, null, null, null));
                }
            }
        }
        
        if (tableRows.isEmpty()) {
            tableRows.add(new TableRow(targetTable, values, null, null, null));
        }
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
            
                log.info("Remove foreign row from table '{}' fk name '{}' where sql '{}' to correct table '{}' for column '{}'",
                        foreignTable.getFullyQualifiedTableName(), foreignTableRow.getFkName(), foreignTableRow.getWhereSql(), 
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

    protected int queryForInt(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, String sql, Object[] values, int[] types) {
        return doInTransaction(platform, databaseWriter, new ITransactionCallback<Integer>() {
            public Integer execute(ISqlTransaction transaction) {
                List<Number> list = transaction.query(sql, new NumberMapper(), values, types);
                if (list.size() > 0) {
                    return list.get(0).intValue();
                }
                return 0;
            }
        });
    }

    private <T> T doInTransaction(IDatabasePlatform platform, DefaultDatabaseWriter databaseWriter, ITransactionCallback<T> callback) {
        ISqlTransaction transaction = null;
        boolean useSavepoints = platform.getDatabaseInfo().isRequiresSavePointsInTransaction();
        boolean isAborted = Boolean.TRUE.equals(databaseWriter.getContext().get(AbstractDatabaseWriter.TRANSACTION_ABORTED));
        try {
            if (useSavepoints && isAborted) {
                transaction = platform.getSqlTemplate().startSqlTransaction(true);
                transaction.prepareAndExecute("select set_config('symmetric.triggers_disabled', '1', false)");
                String sourceNodeId = (String) databaseWriter.getContext().get("sourceNodeId");
                transaction.prepareAndExecute("select set_config('symmetric.node_disabled', '" +
                        (sourceNodeId == null ? "" : sourceNodeId) + "', false)");
            } else {
                transaction = databaseWriter.getTransaction();
                if (useSavepoints) {
                    transaction.execute("savepoint sym");
                }
            }
            return callback.execute(transaction);
        } catch (SqlException e) {
            if (useSavepoints && !isAborted) {
                transaction.execute("rollback to savepoint sym");
            }
            throw e;
        } finally {
            if (useSavepoints) {
                if (isAborted) {
                    transaction.prepareAndExecute("select set_config('symmetric.triggers_disabled', '', false)");
                    transaction.prepareAndExecute("select set_config('symmetric.node_disabled', '', false)");
                    transaction.close();
                } else {
                    transaction.execute("release savepoint sym");
                }
            }
        }
    }
    
    private interface ITransactionCallback<T> {
        public T execute(ISqlTransaction transaction);
    }

}
