/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.exception.ParseException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter.LoadStatus;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {

    protected Logger log = LoggerFactory.getLogger(DefaultDatabaseWriterConflictResolver.class);

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
                        performFallbackToUpdate(writer, data, conflict, true);
                        break;
                    case NEWER_WINS:
                        if ((conflict.getDetectType() == DetectConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflict, writer, data))
                                || (conflict.getDetectType() == DetectConflict.USE_VERSION && isVersionNewer(
                                        conflict, writer, data))) {
                            performFallbackToUpdate(writer, data, conflict, true);
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
                        if (conflict.getDetectType() == DetectConflict.USE_PK_DATA) {
                            CsvData withoutOldData =  data.copyWithoutOldData();
                            try {
                                // we already tried to update using the pk
                                performFallbackToInsert(writer, withoutOldData, conflict, true);
                            } catch (ConflictException ex) {
                                performFallbackToUpdate(writer, withoutOldData, conflict, true);
                            }
                        } else {
                            try {
                                performFallbackToUpdate(writer, data, conflict, true);
                            } catch (ConflictException ex) {
                                performFallbackToInsert(writer, data, conflict, true);
                            }
                        }
                        break;
                    case NEWER_WINS:
                        if ((conflict.getDetectType() == DetectConflict.USE_TIMESTAMP && isTimestampNewer(
                                conflict, writer, data))
                                || (conflict.getDetectType() == DetectConflict.USE_VERSION && isVersionNewer(
                                        conflict, writer, data))) {
                            try {
                                performFallbackToUpdate(writer, data, conflict, false);
                            } catch (ConflictException ex) {
                                performFallbackToInsert(writer, data, conflict, true);
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

            case DELETE:
                switch (conflict.getResolveType()) {
                    case FALLBACK:
                        LoadStatus status = LoadStatus.CONFLICT;
                        if (conflict.getDetectType() != DetectConflict.USE_PK_DATA) {
                            status = writer.delete(data, false);
                        }
                        if (status == LoadStatus.CONFLICT) {
                            writer.getStatistics().get(writer.getBatch())
                                    .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
                        }
                        break;
                    case IGNORE:
                        ignore(writer, conflict);
                        break;
                    case NEWER_WINS:
                        // nothing to do ...
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
                        } else {
                            throw new ConflictException(data, writer.getTargetTable(), false, conflict);
                        }
                        break;

                }
                break;

            default:
                break;
        }
    }

    protected void beforeResolutionAttempt(Conflict conflict) {
    }

    protected void afterResolutionAttempt(Conflict conflict) {
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
                    performFallbackToUpdate(writer, data, conflict, true);
                } catch (ConflictException ex) {
                    performFallbackToInsert(writer, data, conflict, true);
                }
            }
        } else {
            throw new ConflictException(data, writer.getTargetTable(), false, conflict);
        }
    }

    protected boolean isTimestampNewer(Conflict conflict, DatabaseWriter writer, CsvData data) {
        IDatabasePlatform platform = writer.getPlatform();
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, targetTable);
        Column column = targetTable.getColumnWithName(columnName);
        String sql = stmt.getColumnsSql(new Column[] { column });

        Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                CsvData.ROW_DATA);
        String loadingStr = newData.get(columnName);
        
        Date loadingTs = null;
        Date existingTs = null;
        if (column.getMappedTypeCode() == -101) {
            // Get the existingTs with timezone
            String existingStr = writer.getTransaction().queryForObject(sql, String.class,
                    objectValues);
            int split = existingStr.lastIndexOf(" ");
            existingTs = FormatUtils.parseDate(existingStr.substring(0, split).trim(),
                    FormatUtils.TIMESTAMP_PATTERNS,
                    TimeZone.getTimeZone(existingStr.substring(split).trim()));

            // Get the loadingTs with timezone
            split = loadingStr.lastIndexOf(" ");
            loadingTs = FormatUtils.parseDate(loadingStr.substring(0, split).trim(),
                    FormatUtils.TIMESTAMP_PATTERNS,
                    TimeZone.getTimeZone(loadingStr.substring(split).trim()));
        } else {
            // Get the existingTs
            existingTs = writer.getTransaction().queryForObject(sql, Timestamp.class,
                    objectValues);
            // Get the loadingTs
            Object[] values = platform.getObjectValues(writer.getBatch().getBinaryEncoding(),
                    new String[] { loadingStr }, new Column[] { column });
            if (values[0] instanceof Date) {
                loadingTs = (Date) values[0];
            } else {
                throw new ParseException("Could not parse " + columnName + " with a value of "
                        + loadingStr + " for purposes of conflict detection");
            }
        }

        return existingTs == null || loadingTs.after(existingTs);
    }

    protected boolean isVersionNewer(Conflict conflict, DatabaseWriter writer, CsvData data) {
        String columnName = conflict.getDetectExpression();
        Table targetTable = writer.getTargetTable();
        Table sourceTable = writer.getSourceTable();
        String[] pkData = data.getPkData(targetTable);
        Object[] objectValues = writer.getPlatform().getObjectValues(
                writer.getBatch().getBinaryEncoding(), pkData, targetTable.getPrimaryKeyColumns());
        DmlStatement stmt = writer.getPlatform().createDmlStatement(DmlType.FROM, targetTable);
        String sql = stmt.getColumnsSql(new Column[] { targetTable.getColumnWithName(columnName) });
        Long existingVersion = writer.getTransaction()
                .queryForObject(sql, Long.class, objectValues);
        Map<String, String> newData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                CsvData.ROW_DATA);
        Long loadingVersion = Long.valueOf(newData.get(columnName));
        return loadingVersion > existingVersion;
    }

    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data, Conflict conflict, boolean retransform) {
        try {
            beforeResolutionAttempt(conflict);
            LoadStatus loadStatus = writer.update(data, conflict.isResolveChangesOnly(), false);
            if (loadStatus != LoadStatus.SUCCESS) {
                throw new ConflictException(data, writer.getTargetTable(), true, conflict);
            } else {
                writer.getStatistics().get(writer.getBatch())
                        .increment(DataWriterStatisticConstants.FALLBACKUPDATECOUNT);
            }
        } finally {
            afterResolutionAttempt(conflict);
        }
    }

    protected void performFallbackToInsert(DatabaseWriter writer, CsvData csvData, Conflict conflict, boolean retransform) {
        try {
            beforeResolutionAttempt(conflict);
            LoadStatus loadStatus = writer.insert(csvData);
            if (loadStatus != LoadStatus.SUCCESS) {
                throw new ConflictException(csvData, writer.getTargetTable(), true, conflict);
            } else {
                writer.getStatistics().get(writer.getBatch())
                        .increment(DataWriterStatisticConstants.FALLBACKINSERTCOUNT);
            }
        } finally {
            afterResolutionAttempt(conflict);
        }
    }

}
