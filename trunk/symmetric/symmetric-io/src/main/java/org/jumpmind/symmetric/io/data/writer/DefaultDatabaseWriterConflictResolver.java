package org.jumpmind.symmetric.io.data.writer;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter.LoadStatus;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {

    protected Logger log = LoggerFactory.getLogger(getClass());

    public void needsResolved(DatabaseWriter writer, CsvData data, LoadStatus loadStatus) {
        DataEventType originalEventType = data.getDataEventType();
        DatabaseWriterSettings writerSettings = writer.getWriterSettings();
        Conflict conflict = writerSettings.pickConflict(writer.getTargetTable(), writer.getBatch());
        Statistics statistics = writer.getStatistics().get(writer.getBatch());
        long statementCount = statistics.get(DataWriterStatisticConstants.STATEMENTCOUNT);
        long lineNumber = statistics.get(DataWriterStatisticConstants.LINENUMBER);
        ResolvedData resolvedData = writerSettings.getResolvedData(statementCount);

        logConflictHappened(conflict, data, writer, resolvedData, lineNumber);
        
        switch (originalEventType) {
            case INSERT:
                switch (conflict.getResolveType()) {
                    case FALLBACK:
                        performFallbackToUpdate(writer, data, conflict.isResolveChangesOnly());
                        break;
                    case NEWER_WINS:
                        if ((conflict.getDetectType() == DetectConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflict, writer, data))
                                || (conflict.getDetectType() == DetectConflict.USE_VERSION && isVersionNewer(
                                        conflict, writer, data))) {
                            try {
                                performFallbackToUpdate(writer, data, false);
                            } catch (RuntimeException ex) {
                                throw ex;
                            }
                        } else {
                            if (!conflict.isResolveRowOnly()) {
                                throw new IgnoreBatchException();
                            }
                        }
                        break;
                    case IGNORE:
                        ignore(writer, conflict);
                        break;
                    case MANUAL:
                    default:
                        attemptToResolve(resolvedData, data, writer, conflict);
                        break;

                }
                break;

            case UPDATE:
                switch (conflict.getResolveType()) {
                    case FALLBACK:
                        performFallbackToInsert(writer, data);
                        break;
                    case NEWER_WINS:
                        if ((conflict.getDetectType() == DetectConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflict, writer, data))
                                || (conflict.getDetectType() == DetectConflict.USE_VERSION && isVersionNewer(
                                        conflict, writer, data))) {
                            performFallbackToUpdate(writer, data, conflict.isResolveChangesOnly());
                        } else {
                            if (!conflict.isResolveRowOnly()) {
                                throw new IgnoreBatchException();
                            }
                        }
                        break;
                    case IGNORE:
                        ignore(writer, conflict);
                        break;
                    case MANUAL:
                    default:
                        attemptToResolve(resolvedData, data, writer, conflict);
                        break;

                }
                break;

            case DELETE:
                switch (conflict.getResolveType()) {
                    case FALLBACK:
                        writer.getStatistics().get(writer.getBatch())
                        .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
                        break;
                    case IGNORE:
                        ignore(writer, conflict);
                        break;
                    case MANUAL:
                    default:
                        if (resolvedData != null) {
                            if (!resolvedData.isIgnoreRow()) {
                                writer.delete(data, false);
                            } else {
                                if (!conflict.isResolveRowOnly()) {
                                    throw new IgnoreBatchException();
                                }
                            }
                        }
                        break;

                }
                break;

            default:
                break;
        }
    }

    protected void logConflictHappened(Conflict conflict, CsvData data, DatabaseWriter writer,
            ResolvedData resolvedData, long lineNumber) {
        if (log.isDebugEnabled()) {
            log.debug("Conflict detected: {} in batch {} at line {} for table {}", new Object[] {
                    conflict.getConflictId() == null ? "default" : conflict.getConflictId(),
                    writer.getBatch().getBatchId(), lineNumber,
                    writer.getTargetTable().getFullyQualifiedTableName() });
            String csvData = data.getCsvData(CsvData.ROW_DATA);
            if (StringUtils.isNotBlank(csvData)) {
                log.debug("Row data: {}", csvData);
            }

            csvData = data.getCsvData(CsvData.OLD_DATA);
            if (StringUtils.isNotBlank(csvData)) {
                log.debug("Old data: {}", csvData);
            }

            csvData = resolvedData != null ? resolvedData.getResolvedData() : null;
            if (StringUtils.isNotBlank(csvData)) {
                log.debug("Resolve data: {}", csvData);
            }

        }
    }

    protected void ignore(DatabaseWriter writer, Conflict conflict) {
        if (conflict.isResolveRowOnly()) {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.IGNORECOUNT);
        } else {
            throw new IgnoreBatchException();
        }
    }

    protected void attemptToResolve(ResolvedData resolvedData, CsvData data, DatabaseWriter writer,
            Conflict conflict) {
        if (resolvedData != null) {
            if (!resolvedData.isIgnoreRow()) {
                data.putCsvData(CsvData.ROW_DATA, resolvedData.getResolvedData());
                try {
                    performFallbackToUpdate(writer, data, conflict.isResolveChangesOnly());
                } catch (ConflictException ex) {
                    performFallbackToInsert(writer, data);
                }
            }
        } else {
            throw new ConflictException(data, writer.getTargetTable(), false);
        }
    }

    protected boolean isTimestampNewer(Conflict conflictSetting, DatabaseWriter writer, CsvData data) {
        String columnName = conflictSetting.getDetectExpression();
        Table table = writer.getTargetTable();
        String[] pkData = data.getPkData(table);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, table.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, table);
        String sql = stmt.getColumnsSql(new Column[] { table.getColumnWithName(columnName) });
        Timestamp existingTs = writer.getTransaction().queryForObject(sql, Timestamp.class,
                objectValues);
        Map<String, String> newData = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
        Timestamp loadingTs = Timestamp.valueOf(newData.get(columnName));
        return loadingTs.after(existingTs);
    }

    protected boolean isVersionNewer(Conflict conflictSetting, DatabaseWriter writer, CsvData data) {
        String columnName = conflictSetting.getDetectExpression();
        Table table = writer.getTargetTable();
        String[] pkData = data.getPkData(table);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, table.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, table);
        String sql = stmt.getColumnsSql(new Column[] { table.getColumnWithName(columnName) });
        Long existingVersion = writer.getTransaction()
                .queryForObject(sql, Long.class, objectValues);
        Map<String, String> newData = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
        Long loadingVersion = Long.valueOf(newData.get(columnName));
        return loadingVersion > existingVersion;
    }

    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data,
            boolean fallbackChanges) {
        LoadStatus loadStatus = writer.update(data, fallbackChanges, false);
        if (loadStatus != LoadStatus.SUCCESS) {
            throw new ConflictException(data, writer.getTargetTable(), true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKUPDATECOUNT);
        }
    }

    protected void performFallbackToInsert(DatabaseWriter writer, CsvData csvData) {
        LoadStatus loadStatus = writer.insert(csvData);
        if (loadStatus != LoadStatus.SUCCESS) {
            throw new ConflictException(csvData, writer.getTargetTable(), true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKINSERTCOUNT);
        }
    }

}
