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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.load.IReloadVariableFilter;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.route.IDataRouter;
import org.jumpmind.symmetric.route.SimpleRouterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectFromTableSource extends SelectFromSource {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private OutgoingBatch outgoingBatch;
    private List<SelectFromTableEvent> selectFromTableEventsToSend;
    private SelectFromTableEvent currentInitialLoadEvent;
    private ISqlReadCursor<Data> cursor;
    private SimpleRouterContext routingContext;
    private Node node;
    private Set<Node> nodeSet;
    private TriggerRouter triggerRouter;
    private Map<String, IDataRouter> routers;
    private IDataRouter dataRouter;
    private ColumnsAccordingToTriggerHistory columnsAccordingToTriggerHistory;
    private String overrideSelectSql;
    private boolean initialLoadSelectUsed;
    private boolean isSelfReferencingFk;
    private int selfRefLevel;
    private String selfRefParentColumnName;
    private String selfRefChildColumnName;
    private boolean isFirstRow;
    private boolean isLobFirstPass;
    private boolean isConfiguration;
    private boolean isInitialLoadUseColumnTemplates;

    public SelectFromTableSource(ISymmetricEngine engine, OutgoingBatch outgoingBatch, Batch batch, SelectFromTableEvent event) {
        super(engine);
        this.outgoingBatch = outgoingBatch;
        List<SelectFromTableEvent> initialLoadEvents = new ArrayList<SelectFromTableEvent>(1);
        initialLoadEvents.add(event);
        outgoingBatch.resetExtractRowStats();
        init(batch, initialLoadEvents);
    }

    public SelectFromTableSource(ISymmetricEngine engine, Batch batch, List<SelectFromTableEvent> initialLoadEvents) {
        super(engine);
        init(batch, initialLoadEvents);
    }

    protected final void init(Batch batch, List<SelectFromTableEvent> initialLoadEvents) {
        this.batch = batch;
        selectFromTableEventsToSend = new ArrayList<SelectFromTableEvent>(initialLoadEvents);
        node = nodeService.findNode(batch.getTargetNodeId(), true);
        nodeSet = new HashSet<Node>(1);
        nodeSet.add(node);
        routers = engine.getRouterService().getRouters();
        if (node == null) {
            throw new SymmetricException("Could not find a node represented by %s", batch.getTargetNodeId());
        }
        columnsAccordingToTriggerHistory = new ColumnsAccordingToTriggerHistory(engine, nodeService.findIdentity(), node);
        isInitialLoadUseColumnTemplates = parameterService.is(ParameterConstants.INITIAL_LOAD_USE_COLUMN_TEMPLATES_ENABLED);
    }

    public void setConfiguration(boolean isConfiguration) {
        this.isConfiguration = isConfiguration;
    }

    public CsvData next() {
        CsvData data = null;
        do {
            data = selectNext();
        } while (data != null && routingContext != null && !shouldDataBeRouted(data));
        if (data != null && outgoingBatch != null && !outgoingBatch.isExtractJobFlag()) {
            outgoingBatch.incrementExtractRowCount();
            outgoingBatch.incrementExtractRowCount(data.getDataEventType());
        }
        return data;
    }

    public boolean shouldDataBeRouted(CsvData data) {
        DataMetaData dataMetaData = new DataMetaData((Data) data, sourceTable, triggerRouter.getRouter(), routingContext.getChannel());
        Collection<String> nodeIds = dataRouter.routeToNodes(routingContext, dataMetaData, nodeSet, true, initialLoadSelectUsed, triggerRouter);
        return nodeIds != null && nodeIds.contains(node.getNodeId());
    }

    protected CsvData selectNext() {
        CsvData data = null;
        if (currentInitialLoadEvent == null && selectFromTableEventsToSend.size() > 0) {
            currentInitialLoadEvent = selectFromTableEventsToSend.remove(0);
            TriggerHistory history = currentInitialLoadEvent.getTriggerHistory();
            isSelfReferencingFk = false;
            isFirstRow = true;
            if (currentInitialLoadEvent.containsData()) {
                data = currentInitialLoadEvent.getData();
                sourceTable = columnsAccordingToTriggerHistory.lookup(currentInitialLoadEvent.getTriggerRouter().getRouterId(), history, false, true);
                targetTable = columnsAccordingToTriggerHistory.lookup(currentInitialLoadEvent.getTriggerRouter().getRouterId(), history, true, false);
                currentInitialLoadEvent = null;
            } else {
                triggerRouter = currentInitialLoadEvent.getTriggerRouter();
                initialLoadSelectUsed = currentInitialLoadEvent.getInitialLoadSelect() != null && !currentInitialLoadEvent.getInitialLoadSelect().equals("1=1")
                        ? true
                        : StringUtils.isNotBlank(triggerRouter.getInitialLoadSelect());
                Router router = triggerRouter.getRouter();
                if (!StringUtils.isBlank(router.getRouterType())) {
                    dataRouter = routers.get(router.getRouterType());
                }
                if (dataRouter == null) {
                    dataRouter = routers.get("default");
                }
                if (routingContext == null) {
                    NodeChannel channel = batch != null ? configurationService.getNodeChannel(batch.getChannelId(), false)
                            : new NodeChannel(triggerRouter.getTrigger().getChannelId());
                    routingContext = new SimpleRouterContext(batch == null ? null : batch.getTargetNodeId(), channel);
                }
                sourceTable = columnsAccordingToTriggerHistory.lookup(triggerRouter.getRouter().getRouterId(), history, false, true);
                targetTable = columnsAccordingToTriggerHistory.lookup(triggerRouter.getRouter().getRouterId(), history, true, false);
                overrideSelectSql = currentInitialLoadEvent.getInitialLoadSelect();
                if (overrideSelectSql != null && overrideSelectSql.trim().toUpperCase().startsWith("WHERE")) {
                    overrideSelectSql = overrideSelectSql.trim().substring(5);
                }
                if (parameterService.is(ParameterConstants.INITIAL_LOAD_RECURSION_SELF_FK)) {
                    ForeignKey fk = sourceTable.getSelfReferencingForeignKey();
                    if (fk != null) {
                        Reference[] refs = fk.getReferences();
                        if (refs.length == 1) {
                            isSelfReferencingFk = true;
                            selfRefParentColumnName = refs[0].getLocalColumnName();
                            selfRefChildColumnName = refs[0].getForeignColumnName();
                            selfRefLevel = 0;
                            log.info("Ordering rows for table {} using self-referencing foreign key {} -> {}",
                                    sourceTable.getName(), selfRefParentColumnName, selfRefChildColumnName);
                        } else {
                            log.warn("Unable to order rows for self-referencing foreign key because it contains multiple columns");
                        }
                    }
                }
                ISymmetricDialect symmetricDialectToUse = getSymmetricDialect();
                if (routingContext.getChannel().isReloadFlag() && symmetricDialectToUse.isInitialLoadTwoPassLob(sourceTable)) {
                    isLobFirstPass = true;
                }
                startNewCursor(history, triggerRouter);
            }
        }
        if (cursor != null) {
            data = cursor.next();
            if (data == null) {
                closeCursor();
                ISymmetricDialect symmetricDialectToUse = getSymmetricDialect();
                if (isSelfReferencingFk && !isFirstRow) {
                    selfRefLevel++;
                    startNewCursor(currentInitialLoadEvent.getTriggerHistory(), triggerRouter);
                    isFirstRow = true;
                } else if (symmetricDialectToUse.isInitialLoadTwoPassLob(sourceTable) && isLobFirstPass) {
                    isLobFirstPass = false;
                    startNewCursor(currentInitialLoadEvent.getTriggerHistory(), triggerRouter);
                } else {
                    currentInitialLoadEvent = null;
                }
                data = next();
            } else if (isFirstRow) {
                isFirstRow = false;
            }
        }
        return data;
    }

    protected void closeCursor() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    public ISymmetricDialect getSymmetricDialect() {
        ISymmetricDialect dialect = null;
        if (isConfiguration || (sourceTable != null && sourceTable.getNameLowerCase().startsWith(parameterService.getTablePrefix().toLowerCase() + "_"))) {
            dialect = symmetricDialect;
        } else {
            dialect = symmetricDialect.getTargetDialect();
        }
        return dialect;
    }

    protected void startNewCursor(final TriggerHistory triggerHistory, final TriggerRouter triggerRouter) {
        ISymmetricDialect symmetricDialectToUse = getSymmetricDialect();
        String selectSql = overrideSelectSql;
        if (isSelfReferencingFk) {
            if (selectSql == null) {
                selectSql = "";
            } else if (StringUtils.isNotBlank(selectSql)) {
                selectSql += " and ";
            }
            if (selfRefLevel == 0) {
                selectSql += "(" + selfRefParentColumnName + " is null or " + selfRefParentColumnName + " = " + selfRefChildColumnName + ") ";
            } else {
                DatabaseInfo info = symmetricDialectToUse.getPlatform().getDatabaseInfo();
                String tableName = Table.getFullyQualifiedTableName(sourceTable.getCatalog(), sourceTable.getSchema(),
                        sourceTable.getName(), info.getDelimiterToken(), info.getCatalogSeparator(), info.getSchemaSeparator());
                String refSql = "select " + selfRefChildColumnName + " from " + tableName + " where " + selfRefParentColumnName;
                selectSql += selfRefParentColumnName + " in (";
                for (int i = 1; i < selfRefLevel; i++) {
                    selectSql += refSql + " in (";
                }
                selectSql += refSql + " is null or " + selfRefChildColumnName + " = " + selfRefParentColumnName + " ) and " +
                        selfRefParentColumnName + " != " + selfRefChildColumnName + StringUtils.repeat(")", selfRefLevel - 1);
            }
            log.info("Querying level {} for table {}: {}", selfRefLevel, sourceTable.getName(), selectSql);
        }
        Channel channel = configurationService.getChannel(triggerRouter.getTrigger().getReloadChannelId());
        if (channel.isReloadFlag() && symmetricDialectToUse.isInitialLoadTwoPassLob(sourceTable)) {
            channel = new Channel();
            channel.setContainsBigLob(!isLobFirstPass);
            selectSql = symmetricDialectToUse.getInitialLoadTwoPassLobSql(selectSql, sourceTable, isLobFirstPass);
            log.info("Querying {} pass LOB for table {}: {}", (isLobFirstPass ? "first" : "second"), sourceTable.getName(), selectSql);
        }
        String sql = symmetricDialectToUse.createInitialLoadSqlFor(currentInitialLoadEvent.getNode(), triggerRouter,
                sourceTable, triggerHistory, channel, selectSql);
        for (IReloadVariableFilter filter : extensionService.getExtensionPointList(IReloadVariableFilter.class)) {
            sql = filter.filterInitalLoadSql(sql, node, targetTable);
        }
        final String initialLoadSql = sql;
        final int expectedCommaCount = triggerHistory.getParsedColumnNames().length - 1;
        final boolean selectedAsCsv = symmetricDialectToUse.getParameterService().is(ParameterConstants.INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED);
        final boolean objectValuesWillNeedEscaped = !symmetricDialectToUse.getTriggerTemplate().useTriggerTemplateForColumnTemplatesDuringInitialLoad();
        final boolean[] isColumnPositionUsingTemplate = symmetricDialectToUse.getColumnPositionUsingTemplate(sourceTable, triggerHistory);
        final boolean checkRowLength = parameterService.is(ParameterConstants.EXTRACT_CHECK_ROW_SIZE, false);
        final long rowMaxLength = parameterService.getLong(ParameterConstants.EXTRACT_ROW_MAX_LENGTH, 1000000000);
        boolean returnLobObjects = checkRowLength && sourceTable.containsLobColumns(symmetricDialect.getPlatform()) &&
                !sourceTable.getNameLowerCase().startsWith(symmetricDialect.getTablePrefix());
        log.debug(sql);
        cursor = symmetricDialectToUse.getPlatform().getSqlTemplate().queryForCursor(initialLoadSql, new ISqlRowMapper<Data>() {
            public Data mapRow(Row row) {
                if (checkRowLength) {
                    // Account for double byte characters and encoding
                    long rowSize = row.getLength() * 2;
                    if (rowSize > rowMaxLength) {
                        StringBuilder pkValues = new StringBuilder();
                        int i = 0;
                        Object[] rowValues = row.values().toArray();
                        for (String name : sourceTable.getPrimaryKeyColumnNames()) {
                            pkValues.append(name).append("=").append(rowValues[i]);
                            i++;
                        }
                        log.warn("Extract row max size exceeded, keys [" + pkValues.toString() + "], size=" + rowSize);
                        Data data = new Data(0, null, "", DataEventType.SQL, triggerHistory.getSourceTableName(), null,
                                triggerHistory, batch.getChannelId(), null, null);
                        return data;
                    }
                }
                String csvRow = null;
                if (selectedAsCsv) {
                    csvRow = row.stringValue();
                    int commaCount = StringUtils.countMatches(csvRow, ",");
                    if (commaCount < expectedCommaCount) {
                        throw new SymmetricException(
                                "The extracted row data did not have the expected (%d) number of columns (actual=%s): %s.  The initial load sql was: %s",
                                expectedCommaCount, commaCount, csvRow, initialLoadSql);
                    }
                } else if (objectValuesWillNeedEscaped) {
                    csvRow = platform.getCsvStringValue(symmetricDialect.getBinaryEncoding(), sourceTable.getColumns(), row, isColumnPositionUsingTemplate);
                } else {
                    csvRow = row.csvValue();
                }
                Data data = new Data(0, null, csvRow, DataEventType.INSERT, triggerHistory.getSourceTableName(), null,
                        triggerHistory, batch.getChannelId(), null, null);
                return data;
            }
        }, returnLobObjects);
    }

    public boolean requiresLobsSelectedFromSource(CsvData data) {
        if (isInitialLoadUseColumnTemplates && currentInitialLoadEvent != null && currentInitialLoadEvent.getTriggerRouter() != null) {
            if (currentInitialLoadEvent.getTriggerRouter().getTrigger().isUseStreamLobs()
                    || (data != null && hasLobsThatNeedExtract(sourceTable, data))) {
                return true;
            }
            return currentInitialLoadEvent.getTriggerRouter().getTrigger().isUseStreamLobs();
        } else {
            return false;
        }
    }

    public void close() {
        closeCursor();
    }
}
