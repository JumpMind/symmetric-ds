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
package org.jumpmind.symmetric.extract;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.ProtocolException;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.TableReloadStatus;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.AbstractFileParsingRouter;
import org.jumpmind.symmetric.util.CounterStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectFromSymDataSource extends SelectFromSource {
    private final Logger log = LoggerFactory.getLogger(getClass());
    protected OutgoingBatch outgoingBatch;
    protected TriggerHistory lastTriggerHistory;
    protected String lastRouterId;
    protected boolean requiresLobSelectedFromSource;
    protected ISqlReadCursor<Data> cursor;
    protected SelectFromTableSource reloadSource;
    protected Node targetNode;
    protected ProcessInfo processInfo;
    protected ColumnsAccordingToTriggerHistory columnsAccordingToTriggerHistory;
    protected Map<Integer, TriggerRouter> triggerRoutersByTriggerHist;
    protected Map<Integer, CounterStat> missingTriggerRoutersByTriggerHist = new HashMap<Integer, CounterStat>();
    protected boolean containsBigLob;
    protected boolean dialectHasNoOldBinaryData;

    public SelectFromSymDataSource(ISymmetricEngine engine, OutgoingBatch outgoingBatch, Node sourceNode, Node targetNode,
            ProcessInfo processInfo, boolean containsBigLob) {
        super(engine);
        this.outgoingBatch = outgoingBatch;
        this.processInfo = processInfo;
        this.targetNode = targetNode;
        this.containsBigLob = containsBigLob;
        batch = new Batch(BatchType.EXTRACT, outgoingBatch.getBatchId(), outgoingBatch.getChannelId(), symmetricDialect.getBinaryEncoding(),
                sourceNode.getNodeId(), outgoingBatch.getNodeId(), outgoingBatch.isCommonFlag());
        columnsAccordingToTriggerHistory = new ColumnsAccordingToTriggerHistory(engine, sourceNode, targetNode);
        outgoingBatch.resetExtractRowStats();
        triggerRoutersByTriggerHist = triggerRouterService.getTriggerRoutersByTriggerHist(targetNode.getNodeGroupId(), false);
        dialectHasNoOldBinaryData = symmetricDialect.getName().equals(DatabaseNamesConstants.MSSQL2000)
                || symmetricDialect.getName().equals(DatabaseNamesConstants.MSSQL2005)
                || symmetricDialect.getName().equals(DatabaseNamesConstants.MSSQL2008)
                || symmetricDialect.getName().equals(DatabaseNamesConstants.MSSQL2016);
    }

    public CsvData next() {
        if (cursor == null) {
            cursor = dataService.selectDataFor(batch.getBatchId(), batch.getTargetNodeId(), containsBigLob);
        }
        Data data = null;
        if (reloadSource != null) {
            data = (Data) reloadSource.next();
            targetTable = reloadSource.getTargetTable();
            sourceTable = reloadSource.getSourceTable();
            if (data == null) {
                reloadSource.close();
                reloadSource = null;
            } else {
                requiresLobSelectedFromSource = reloadSource.requiresLobsSelectedFromSource(data);
            }
            lastTriggerHistory = null;
        }
        if (data == null) {
            data = cursor.next();
            if (data != null) {
                TriggerHistory triggerHistory = null;
                TriggerRouter triggerRouter = null;
                do {
                    triggerHistory = data.getTriggerHistory();
                    triggerRouter = triggerRoutersByTriggerHist.get(triggerHistory.getTriggerHistoryId());
                    if (triggerRouter == null) {
                        CounterStat counterStat = missingTriggerRoutersByTriggerHist.get(triggerHistory.getTriggerHistoryId());
                        if (counterStat == null) {
                            triggerRouter = triggerRouterService.getTriggerRouterByTriggerHist(targetNode.getNodeGroupId(),
                                    triggerHistory.getTriggerHistoryId(), true);
                            if (triggerRouter == null) {
                                counterStat = new CounterStat(data.getDataId(), 1);
                                missingTriggerRoutersByTriggerHist.put(triggerHistory.getTriggerHistoryId(), counterStat);
                                data = cursor.next();
                            }
                        } else {
                            counterStat.incrementCount();
                            data = cursor.next();
                        }
                        if (data == null) {
                            closeCursor();
                            return null;
                        }
                        triggerRoutersByTriggerHist.put(triggerHistory.getTriggerHistoryId(), triggerRouter);
                    }
                } while (triggerRouter == null);
                String routerId = triggerRouter.getRouterId();
                if (data.getDataEventType() == DataEventType.RELOAD) {
                    data = processReloadEvent(triggerHistory, triggerRouter, data);
                } else {
                    Trigger trigger = triggerRouter.getTrigger();
                    boolean isFileParserRouter = triggerHistory.getTriggerId().equals(AbstractFileParsingRouter.TRIGGER_ID_FILE_PARSER);
                    if (lastTriggerHistory == null || lastTriggerHistory.getTriggerHistoryId() != triggerHistory.getTriggerHistoryId() ||
                            lastRouterId == null || !lastRouterId.equals(routerId)) {
                        sourceTable = columnsAccordingToTriggerHistory.lookup(routerId, triggerHistory, false, !isFileParserRouter, false, true);
                        targetTable = columnsAccordingToTriggerHistory.lookup(routerId, triggerHistory, true, false, false, true);
                        if (trigger != null && trigger.isUseStreamLobs() || (data.getRowData() != null && hasLobsThatNeedExtract(sourceTable, data))) {
                            requiresLobSelectedFromSource = true;
                        } else {
                            requiresLobSelectedFromSource = false;
                        }
                    }
                    data.setNoBinaryOldData(requiresLobSelectedFromSource || dialectHasNoOldBinaryData);
                    outgoingBatch.incrementExtractRowCount();
                    outgoingBatch.incrementExtractRowCount(data.getDataEventType());
                    if (data.getDataEventType().equals(DataEventType.INSERT) || data.getDataEventType().equals(DataEventType.UPDATE)) {
                        int expectedCommaCount = triggerHistory.getParsedColumnNames().length;
                        int commaCount = StringUtils.countMatches(data.getRowData(), ",") + 1;
                        if (commaCount < expectedCommaCount) {
                            String message = "The extracted row for table %s had %d columns but expected %d.  ";
                            if (containsBigLob) {
                                message += "Corrupted row for data ID " + data.getDataId() + ": " + data.getRowData();
                            } else {
                                message += "If this happens often, it might be better to isolate the table with sym_channel.contains_big_lobs enabled.";
                            }
                            throw new ProtocolException(message, data.getTableName(), commaCount, expectedCommaCount);
                        }
                    }
                    if (data.getDataEventType() == DataEventType.CREATE && StringUtils.isBlank(data.getCsvData(CsvData.ROW_DATA))) {
                        if (!processCreateEvent(triggerHistory, routerId, data)) {
                            return null;
                        }
                    }
                }
                if (data != null) {
                    lastTriggerHistory = data.getTriggerHistory();
                    lastRouterId = routerId;
                }
            } else {
                closeCursor();
            }
        }
        return data;
    }

    protected Data processReloadEvent(TriggerHistory triggerHistory, TriggerRouter triggerRouter, Data data) {
        processInfo.setCurrentTableName(triggerHistory.getSourceTableName());
        String initialLoadSelect = data.getRowData();
        if (initialLoadSelect == null && triggerRouter.getTrigger().isStreamRow()) {
            sourceTable = columnsAccordingToTriggerHistory.lookup(triggerRouter.getRouter().getRouterId(), triggerHistory, false, true, false, false);
            Column[] columns = sourceTable.getPrimaryKeyColumns();
            String[] pkData = data.getParsedData(CsvData.PK_DATA);
            boolean[] nullKeyValues = new boolean[columns.length];
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                nullKeyValues[i] = !column.isRequired() && pkData[i] == null;
            }
            DmlStatement dmlStmt = platform.createDmlStatement(DmlType.WHERE, sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), sourceTable.getPrimaryKeyColumns(), sourceTable.getColumns(), nullKeyValues, null);
            Row row = new Row(columns.length);
            for (int i = 0; i < columns.length; i++) {
                row.put(columns[i].getName(), pkData[i]);
            }
            initialLoadSelect = dmlStmt.buildDynamicSql(batch.getBinaryEncoding(), row, false, true, columns);
            if (initialLoadSelect.endsWith(platform.getDatabaseInfo().getSqlCommandDelimiter())) {
                initialLoadSelect = initialLoadSelect.substring(0,
                        initialLoadSelect.length() - platform.getDatabaseInfo().getSqlCommandDelimiter().length());
            }
        }
        SelectFromTableEvent event = new SelectFromTableEvent(targetNode, triggerRouter, triggerHistory, initialLoadSelect);
        reloadSource = createSelectFromTableSource(event);
        data = (Data) reloadSource.next();
        sourceTable = reloadSource.getSourceTable();
        targetTable = reloadSource.getTargetTable();
        requiresLobSelectedFromSource = reloadSource.requiresLobsSelectedFromSource(data);
        if (data == null) {
            data = (Data) next();
        }
        return data;
    }

    protected SelectFromTableSource createSelectFromTableSource(SelectFromTableEvent event) {
        return new SelectFromTableSource(engine, outgoingBatch, batch, event);
    }

    protected boolean processCreateEvent(TriggerHistory triggerHistory, String routerId, Data data) {
        String oldData = data.getCsvData(CsvData.OLD_DATA);
        boolean sendSchemaExcludeIndices = false;
        boolean sendSchemaExcludeForeignKeys = false;
        boolean sendSchemaExcludeDefaults = false;
        if (oldData != null && oldData.length() > 0) {
            String[] excludes = data.getCsvData(CsvData.OLD_DATA).split(",");
            for (String exclude : excludes) {
                if (Constants.SEND_SCHEMA_EXCLUDE_INDICES.equals(exclude)) {
                    sendSchemaExcludeIndices = true;
                } else if (Constants.SEND_SCHEMA_EXCLUDE_FOREIGN_KEYS.equals(exclude)) {
                    sendSchemaExcludeForeignKeys = true;
                } else if (Constants.SEND_SCHEMA_EXCLUDE_DEFAULTS.equals(exclude)) {
                    sendSchemaExcludeDefaults = true;
                }
            }
        }
        boolean excludeDefaults = parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_DEFAULTS, false) | sendSchemaExcludeDefaults;
        boolean excludeForeignKeys = parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_FOREIGN_KEYS, false) | sendSchemaExcludeForeignKeys;
        boolean excludeIndexes = parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_INDEXES, false) | sendSchemaExcludeIndices;
        boolean deferConstraints = outgoingBatch.isLoadFlag() && parameterService.is(ParameterConstants.INITIAL_LOAD_DEFER_CREATE_CONSTRAINTS, false);
        String[] pkData = data.getParsedData(CsvData.PK_DATA);
        if (pkData != null && pkData.length > 0) {
            outgoingBatch.setLoadId(Long.parseLong(pkData[0]));
            TableReloadStatus tableReloadStatus = dataService.getTableReloadStatusByLoadId(outgoingBatch.getLoadId());
            if (tableReloadStatus != null && tableReloadStatus.isCompleted()) {
                // Ignore create table (indexes and foreign keys) at end of load if it was cancelled
                return false;
            }
        }
        /*
         * Force a reread of table so new columns are picked up. A create event is usually sent after there is a change to the table so we want to make sure
         * that the cache is updated
         */
        sourceTable = symmetricDialect.getTargetDialect().getPlatform().getTableFromCache(sourceTable.getCatalog(),
                sourceTable.getSchema(), sourceTable.getName(), true);
        targetTable = columnsAccordingToTriggerHistory.lookup(routerId, triggerHistory, true, true, true, false);
        Table copyTargetTable = targetTable.copy();
        Database db = new Database();
        db.setName("dataextractor");
        db.setCatalog(copyTargetTable.getCatalog());
        db.setSchema(copyTargetTable.getSchema());
        db.addTable(copyTargetTable);
        if (excludeDefaults) {
            copyTargetTable.removeAllColumnDefaults();
        }
        if (excludeForeignKeys || deferConstraints) {
            copyTargetTable.removeAllForeignKeys();
        }
        if (excludeIndexes || deferConstraints) {
            copyTargetTable.removeAllIndexes();
        }
        if (parameterService.is(ParameterConstants.CREATE_TABLE_WITHOUT_PK_IF_SOURCE_WITHOUT_PK, false)
                && sourceTable.getPrimaryKeyColumnCount() == 0 && copyTargetTable.getPrimaryKeyColumnCount() > 0) {
            for (Column column : copyTargetTable.getColumns()) {
                column.setPrimaryKey(false);
            }
        }
        if (parameterService.is(ParameterConstants.MYSQL_TINYINT_DDL_TO_BOOLEAN, false)) {
            for (Column column : copyTargetTable.getColumns()) {
                if (column.getJdbcTypeCode() == Types.TINYINT) {
                    column.setJdbcTypeCode(Types.BOOLEAN);
                    column.setMappedTypeCode(Types.BOOLEAN);
                }
            }
        }
        
        if(parameterService.is(ParameterConstants.DBDIALECT_SYBASE_ASE_CONVERT_UNITYPES_FOR_SYNC)) {
            for(Column column : copyTargetTable.getColumns()) {
                Map<String,PlatformColumn> platformColumns = column.getPlatformColumns();
                String platformColumnType = platformColumns.get("ase").getType();
                if(platformColumnType.equalsIgnoreCase("UNITEXT")) {
                    column.setMappedType("CLOB");
                    column.setMappedTypeCode(Types.CLOB);
                } else if(platformColumnType.equalsIgnoreCase("UNICHAR")) {
                    column.setMappedType("CHAR");
                    column.setMappedTypeCode(Types.CHAR);
                } else if(platformColumnType.equalsIgnoreCase("UNIVARCHAR")) {
                    column.setMappedType("VARCHAR");
                    column.setMappedTypeCode(Types.VARCHAR);
                }
            }
        }
        
        data.setRowData(CsvUtils.escapeCsvData(DatabaseXmlUtil.toXml(db)));
        return true;
    }

    public boolean requiresLobsSelectedFromSource(CsvData data) {
        return requiresLobSelectedFromSource;
    }

    protected void closeCursor() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    public void close() {
        closeCursor();
        if (reloadSource != null) {
            reloadSource.close();
        }
        for (Map.Entry<Integer, CounterStat> entry : missingTriggerRoutersByTriggerHist.entrySet()) {
            log.warn("Could not find trigger router for trigger hist of {}.  Skipped {} events starting with data id of {}",
                    entry.getKey(), entry.getValue().getCount(), entry.getValue().getObject());
        }
    }
}
