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

package org.jumpmind.symmetric.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.DataTruncation;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.job.PushHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * @see IDataService
 */
public class DataService extends AbstractService implements IDataService {

    private String deploymentType;

    private ITriggerRouterService triggerRouterService;

    private INodeService nodeService;

    private IPurgeService purgeService;

    private IConfigurationService configurationService;

    private IOutgoingBatchService outgoingBatchService;

    private List<IReloadListener> reloadListeners;

    private List<IHeartbeatListener> heartbeatListeners;

    private IStatisticManager statisticManager;

    private DataMapper dataMapper;

    public DataService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            String deploymentType, ITriggerRouterService triggerRouterService,
            INodeService nodeService, IPurgeService purgeService,
            IConfigurationService configurationService, IOutgoingBatchService outgoingBatchService,
            IStatisticManager statisticManager) {
        super(parameterService, symmetricDialect);
        this.deploymentType = deploymentType;
        this.triggerRouterService = triggerRouterService;
        this.nodeService = nodeService;
        this.purgeService = purgeService;
        this.configurationService = configurationService;
        this.outgoingBatchService = outgoingBatchService;
        this.statisticManager = statisticManager;
        this.reloadListeners = new ArrayList<IReloadListener>();
        this.heartbeatListeners = new ArrayList<IHeartbeatListener>();
        this.heartbeatListeners.add(new PushHeartbeatListener(parameterService, this, nodeService,
                symmetricDialect));
        this.dataMapper = new DataMapper();

        setSqlMap(new DataServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    protected Map<IHeartbeatListener, Long> lastHeartbeatTimestamps = new HashMap<IHeartbeatListener, Long>();

    public void insertReloadEvent(final Node targetNode, final TriggerRouter triggerRouter) {
        insertReloadEvent(targetNode, triggerRouter, null);
    }

    public void insertReloadEvent(Node targetNode, TriggerRouter triggerRouter,
            String overrideInitialLoadSelect) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertReloadEvent(transaction, targetNode, triggerRouter, null,
                    overrideInitialLoadSelect);
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    public void insertReloadEvent(ISqlTransaction transaction, Node targetNode,
            TriggerRouter triggerRouter, TriggerHistory triggerHistory,
            String overrideInitialLoadSelect) {

        if (triggerHistory == null) {
            triggerHistory = lookupTriggerHistory(triggerRouter.getTrigger());
        }

        // initial_load_select for table can be overridden by populating the
        // row_data
        Data data = new Data(triggerHistory.getSourceTableName(), DataEventType.RELOAD,
                overrideInitialLoadSelect != null ? overrideInitialLoadSelect
                        : triggerRouter.getInitialLoadSelect(), null, triggerHistory, triggerRouter
                        .getTrigger().getChannelId(), null, null);
        insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                triggerRouter.getRouter().getRouterId(), true);
    }

    public void insertReloadEvents(Node targetNode, boolean reverse) {
        // outgoing data events are pointless because we are reloading all
        // data
        outgoingBatchService.markAllAsSentForNode(targetNode);

        Node sourceNode = nodeService.findIdentity();

        boolean transactional = parameterService
                .is(ParameterConstants.DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL);

        ISqlTransaction transaction = null;
        try {

            transaction = platform.getSqlTemplate().startSqlTransaction();

            if (reloadListeners != null) {
                for (IReloadListener listener : reloadListeners) {
                    listener.beforeReload(transaction, targetNode);

                    if (!transactional) {
                        transaction.commit();
                    }
                }
            }

            if (!reverse) {
                // insert node security so the client doing the initial load
                // knows
                // that an initial load is currently happening
                insertNodeSecurityUpdate(transaction, targetNode, true);
            }

            List<TriggerHistory> triggerHistories = triggerRouterService
                    .getActiveTriggerHistories();

            Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = fillTriggerRoutersByHistIdAndSortHist(
                    sourceNode, targetNode, triggerHistories);

            if (parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD)) {
                for (TriggerHistory triggerHistory : triggerHistories) {
                    List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId
                            .get(triggerHistory.getTriggerHistoryId());
                    for (TriggerRouter triggerRouter : triggerRouters) {
                        if (triggerRouter.getInitialLoadOrder() >= 0) {
                            String xml = symmetricDialect.getCreateTableXML(triggerRouter);
                            insertCreateEvent(transaction, targetNode, triggerRouter, xml, true);
                            if (!transactional) {
                                transaction.commit();
                            }
                        }
                    }
                }
            }

            if (parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
                for (ListIterator<TriggerHistory> triggerHistoryIterator = triggerHistories
                        .listIterator(triggerHistories.size()); triggerHistoryIterator
                        .hasPrevious();) {
                    TriggerHistory triggerHistory = triggerHistoryIterator.previous();
                    List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId
                            .get(triggerHistory.getTriggerHistoryId());
                    for (ListIterator<TriggerRouter> iterator = triggerRouters
                            .listIterator(triggerRouters.size()); iterator.hasPrevious();) {
                        TriggerRouter triggerRouter = iterator.previous();
                        if (triggerRouter.getInitialLoadOrder() >= 0) {
                            insertPurgeEvent(transaction, targetNode, triggerRouter, true);
                            if (!transactional) {
                                transaction.commit();
                            }
                        }
                    }
                }
            }

            for (TriggerHistory triggerHistory : triggerHistories) {
                List<TriggerRouter> triggerRouters = triggerRoutersByHistoryId.get(triggerHistory
                        .getTriggerHistoryId());
                for (TriggerRouter triggerRouter : triggerRouters) {
                    if (triggerRouter.getInitialLoadOrder() >= 0) {
                        insertReloadEvent(transaction, targetNode, triggerRouter, triggerHistory,
                                null);
                        if (!transactional) {
                            transaction.commit();
                        }
                    }
                }
            }

            if (reloadListeners != null) {
                for (IReloadListener listener : reloadListeners) {
                    listener.afterReload(transaction, targetNode);
                    if (!transactional) {
                        transaction.commit();
                    }
                }
            }

            if (!reverse) {
                nodeService.setInitialLoadEnabled(transaction, targetNode.getNodeId(), false);

                // don't mark this batch as a load batch so it is forced to go
                // last
                insertNodeSecurityUpdate(transaction, targetNode,
                        parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL));
            }

            statisticManager.incrementNodesLoaded(1);

            transaction.commit();
        } catch (RuntimeException ex) {
            transaction.rollback();
            throw ex;
        } finally {
            close(transaction);
        }

        if (!reverse) {
            // remove all incoming events from the node are starting a reload
            // for
            purgeService.purgeAllIncomingEventsForNode(targetNode.getNodeId());
        }

    }

    private TriggerHistory lookupTriggerHistory(Trigger trigger) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(
                trigger.getTriggerId(), trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName());

        if (history == null) {
            triggerRouterService.syncTriggers();
            history = triggerRouterService.getNewestTriggerHistoryForTrigger(
                    trigger.getTriggerId(), null, null, null);
        }

        if (history == null) {
            throw new RuntimeException("Cannot find history for trigger " + trigger.getTriggerId()
                    + ", " + trigger.getSourceTableName());
        }
        return history;
    }

    public void insertPurgeEvent(Node targetNode, TriggerRouter triggerRouter, boolean isLoad) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertPurgeEvent(transaction, targetNode, triggerRouter, isLoad);
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    protected void insertPurgeEvent(ISqlTransaction transaction, final Node targetNode,
            final TriggerRouter triggerRouter, boolean isLoad) {

        String sql = symmetricDialect.createPurgeSqlFor(targetNode, triggerRouter);
        Trigger trigger = triggerRouter.getTrigger();
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(
                trigger.getTriggerId(), trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName());
        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, triggerRouter.getTrigger()
                        .getChannelId(), null, null);
        insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                triggerRouter.getRouter().getRouterId(), isLoad);
    }

    public void insertSqlEvent(final Node targetNode, final Trigger trigger, String sql,
            boolean isLoad) {
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(
                trigger.getTriggerId(), trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName());
        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, trigger.getChannelId(), null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    private TriggerHistory findTriggerHistoryForGenericSync() {
        String triggerTableName = TableConstants.getTableName(tablePrefix,
                TableConstants.SYM_TRIGGER);
        TriggerHistory history = triggerRouterService.findTriggerHistory(triggerTableName
                .toUpperCase());
        if (history == null) {
            history = triggerRouterService.findTriggerHistory(triggerTableName);
        }
        return history;
    }

    public void insertSqlEvent(final Node targetNode, String sql, boolean isLoad) {
        TriggerHistory history = findTriggerHistoryForGenericSync();
        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public void insertSqlEvent(ISqlTransaction transaction, Node targetNode, String sql,
            boolean isLoad) {
        TriggerHistory history = findTriggerHistoryForGenericSync();
        Data data = new Data(history.getSourceTableName(), DataEventType.SQL,
                CsvUtils.escapeCsvData(sql), null, history, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public int countDataInRange(long firstDataId, long secondDataId) {
        return sqlTemplate.queryForInt(getSql("countDataInRangeSql"), firstDataId, secondDataId);
    }

    public void checkForAndUpdateMissingChannelIds(long firstDataId, long lastDataId) {
        int numberUpdated = sqlTemplate.update(getSql("checkForAndUpdateMissingChannelIdSql"),
                Constants.CHANNEL_DEFAULT, firstDataId, lastDataId);
        if (numberUpdated > 0) {
            log.warn(
                    "There were {} data records found between {} and {} that an invalid channel_id.  Updating them to be on the '{}' channel.",
                    new Object[] { numberUpdated, firstDataId, lastDataId,
                            Constants.CHANNEL_DEFAULT });
        }
    }

    public void insertCreateEvent(final Node targetNode, final TriggerRouter triggerRouter,
            String xml, boolean isLoad) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertCreateEvent(transaction, targetNode, triggerRouter, xml, isLoad);
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    public void insertCreateEvent(ISqlTransaction transaction, final Node targetNode,
            final TriggerRouter triggerRouter, String xml, boolean isLoad) {
        Trigger trigger = triggerRouter.getTrigger();
        TriggerHistory history = triggerRouterService.getNewestTriggerHistoryForTrigger(
                trigger.getTriggerId(), trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName());
        Data data = new Data(
                triggerRouter.getTrigger().getSourceTableName(),
                DataEventType.CREATE,
                CsvUtils.escapeCsvData(xml),
                null,
                history,
                parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL) && isLoad ? Constants.CHANNEL_RELOAD
                        : triggerRouter.getTrigger().getChannelId(), null, null);
        try {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, targetNode.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isLoad);
        } catch (UniqueKeyException e) {
            if (e.getRootCause() != null && e.getRootCause() instanceof DataTruncation) {
                log.error("Table data definition XML was too large and failed.  The feature to send table creates during the initial load may be limited on your platform.  You may need to set the initial.load.create.first parameter to false.");
            }
            throw e;
        }
    }

    public long insertData(Data data) {
        ISqlTransaction transaction = null;
        long dataId = -1;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            dataId = insertData(transaction, data);
            transaction.commit();
            return dataId;
        } finally {
            close(transaction);
        }
    }

    protected long insertData(ISqlTransaction transaction, final Data data) {
        long id = transaction.insertWithGeneratedKey(
                getSql("insertIntoDataSql"),
                symmetricDialect.getSequenceKeyName(SequenceIdentifier.DATA),
                symmetricDialect.getSequenceName(SequenceIdentifier.DATA),
                new Object[] {
                        data.getTableName(),
                        data.getDataEventType().getCode(),
                        data.getRowData(),
                        data.getPkData(),
                        data.getOldData(),
                        data.getTriggerHistory() != null ? data.getTriggerHistory()
                                .getTriggerHistoryId() : -1, data.getChannelId() }, new int[] {
                        Types.VARCHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.NUMERIC, Types.VARCHAR });
        data.setDataId(id);
        return id;
    }

    protected void insertDataEvent(ISqlTransaction transaction, DataEvent dataEvent) {
        this.insertDataEvent(transaction, dataEvent.getDataId(), dataEvent.getBatchId(),
                dataEvent.getRouterId());
    }

    protected void insertDataEvent(ISqlTransaction transaction, long dataId, long batchId,
            String routerId) {
        try {
            transaction
                    .prepareAndExecute(getSql("insertIntoDataEventSql"),
                            new Object[] {
                                    dataId,
                                    batchId,
                                    StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID
                                            : routerId }, new int[] { Types.NUMERIC, Types.NUMERIC,
                                    Types.VARCHAR });
        } catch (RuntimeException ex) {
            log.error("Could not insert a data event: data_id={} batch_id={} router_id={}",
                    new Object[] { dataId, batchId, routerId });
            log.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    public void insertDataEvents(ISqlTransaction transaction, final List<DataEvent> events) {
        if (events.size() > 0) {
            transaction.prepare(getSql("insertIntoDataEventSql"));
            for (DataEvent dataEvent : events) {
                String routerId = dataEvent.getRouterId();
                transaction.addRow(
                        dataEvent,
                        new Object[] {
                                dataEvent.getDataId(),
                                dataEvent.getBatchId(),
                                StringUtils.isBlank(routerId) ? Constants.UNKNOWN_ROUTER_ID
                                        : routerId }, new int[] { Types.NUMERIC, Types.NUMERIC,
                                Types.VARCHAR });
            }
            transaction.flush();
        }
    }

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String channelId,
            List<Node> nodes, String routerId, boolean isLoad) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            long dataId = insertData(transaction, data);
            for (Node node : nodes) {
                insertDataEventAndOutgoingBatch(transaction, dataId, channelId, node.getNodeId(),
                        data.getDataEventType(), routerId, isLoad);
            }
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String nodeId, String routerId,
            boolean isLoad) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertDataAndDataEventAndOutgoingBatch(transaction, data, nodeId, routerId, isLoad);
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    protected void insertDataAndDataEventAndOutgoingBatch(ISqlTransaction transaction, Data data,
            String nodeId, String routerId, boolean isLoad) {
        long dataId = insertData(transaction, data);
        insertDataEventAndOutgoingBatch(transaction, dataId, data.getChannelId(), nodeId,
                data.getDataEventType(), routerId, isLoad);
    }

    protected void insertDataEventAndOutgoingBatch(ISqlTransaction transaction, long dataId,
            String channelId, String nodeId, DataEventType eventType, String routerId,
            boolean isLoad) {
        OutgoingBatch outgoingBatch = new OutgoingBatch(
                nodeId,
                parameterService.is(ParameterConstants.INITIAL_LOAD_USE_RELOAD_CHANNEL) && isLoad ? Constants.CHANNEL_RELOAD
                        : channelId, Status.NE);
        outgoingBatch.setLoadFlag(isLoad);
        outgoingBatch.incrementEventCount(eventType);
        outgoingBatchService.insertOutgoingBatch(transaction, outgoingBatch);
        insertDataEvent(transaction, new DataEvent(dataId, outgoingBatch.getBatchId(), routerId));
    }

    public String reloadNode(String nodeId) {
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return String.format("Unknown node %s", nodeId);
        }
        if (nodeService.setInitialLoadEnabled(nodeId, true)) {
            return String.format("Successfully opened initial load for node %s", nodeId);
        } else {
            return String.format("Could not open initial load for %s", nodeId);
        }
    }

    private Map<Integer, List<TriggerRouter>> fillTriggerRoutersByHistIdAndSortHist(
            Node sourceNode, Node targetNode, List<TriggerHistory> triggerHistories) {

        List<TriggerRouter> triggerRouters = new ArrayList<TriggerRouter>(
                triggerRouterService.getAllTriggerRoutersForReloadForCurrentNode(
                        sourceNode.getNodeGroupId(), targetNode.getNodeGroupId()));

        final Map<Integer, List<TriggerRouter>> triggerRoutersByHistoryId = new HashMap<Integer, List<TriggerRouter>>(
                triggerHistories.size());

        for (TriggerHistory triggerHistory : triggerHistories) {
            List<TriggerRouter> triggerRoutersForTriggerHistory = new ArrayList<TriggerRouter>();
            triggerRoutersByHistoryId.put(triggerHistory.getTriggerHistoryId(),
                    triggerRoutersForTriggerHistory);

            String triggerId = triggerHistory.getTriggerId();
            for (TriggerRouter triggerRouter : triggerRouters) {
                if (triggerRouter.getTrigger().getTriggerId().equals(triggerId)) {
                    triggerRoutersForTriggerHistory.add(triggerRouter);
                }
            }
        }

        final List<Table> sortedTables = getSortedTablesFor(triggerHistories);

        Comparator<TriggerHistory> comparator = new Comparator<TriggerHistory>() {
            public int compare(TriggerHistory o1, TriggerHistory o2) {
                List<TriggerRouter> triggerRoutersForTriggerHist1 = triggerRoutersByHistoryId
                        .get(o1.getTriggerHistoryId());
                int intialLoadOrder1 = 0;
                for (TriggerRouter triggerRouter1 : triggerRoutersForTriggerHist1) {
                    if (triggerRouter1.getInitialLoadOrder() > intialLoadOrder1) {
                        intialLoadOrder1 = triggerRouter1.getInitialLoadOrder();
                    }
                }

                List<TriggerRouter> triggerRoutersForTriggerHist2 = triggerRoutersByHistoryId
                        .get(o2.getTriggerHistoryId());
                int intialLoadOrder2 = 0;
                for (TriggerRouter triggerRouter2 : triggerRoutersForTriggerHist2) {
                    if (triggerRouter2.getInitialLoadOrder() > intialLoadOrder2) {
                        intialLoadOrder2 = triggerRouter2.getInitialLoadOrder();
                    }
                }

                if (intialLoadOrder1 < intialLoadOrder2) {
                    return -1;
                } else if (intialLoadOrder1 > intialLoadOrder2) {
                    return 1;
                }

                Table table1 = platform.getTableFromCache(o1.getSourceCatalogName(),
                        o1.getSourceSchemaName(), o1.getSourceTableName(), false);
                Table table2 = platform.getTableFromCache(o2.getSourceCatalogName(),
                        o2.getSourceSchemaName(), o2.getSourceTableName(), false);

                return new Integer(sortedTables.indexOf(table1)).compareTo(new Integer(sortedTables
                        .indexOf(table2)));
            };
        };

        Collections.sort(triggerHistories, comparator);

        return triggerRoutersByHistoryId;

    }

    protected List<Table> getSortedTablesFor(List<TriggerHistory> histories) {
        List<Table> tables = new ArrayList<Table>(histories.size());
        for (TriggerHistory triggerHistory : histories) {
            Table table = platform.getTableFromCache(triggerHistory.getSourceCatalogName(),
                    triggerHistory.getSourceSchemaName(), triggerHistory.getSourceTableName(),
                    false);
            if (table != null) {
                tables.add(table);
            }
        }
        return Database.sortByForeignKeys(tables);
    }

    private void insertNodeSecurityUpdate(ISqlTransaction transaction, Node node, boolean isReload) {
        Data data = createData(transaction, null, null, tablePrefix + "_node_security",
                " t.node_id = '" + node.getNodeId() + "'");
        if (data != null) {
            insertDataAndDataEventAndOutgoingBatch(transaction, data, node.getNodeId(),
                    Constants.UNKNOWN_ROUTER_ID, isReload);
        }
    }

    public void sendScript(String nodeId, String script, boolean isLoad) {
        Node targetNode = nodeService.findNode(nodeId);
        TriggerHistory history = findTriggerHistoryForGenericSync();
        Data data = new Data(history.getSourceTableName(), DataEventType.BSH,
                CsvUtils.escapeCsvData(script), null, history, Constants.CHANNEL_CONFIG, null, null);
        insertDataAndDataEventAndOutgoingBatch(data, targetNode.getNodeId(),
                Constants.UNKNOWN_ROUTER_ID, isLoad);
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql, boolean isLoad) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {
            return "Unknown node " + nodeId;
        }

        Set<TriggerRouter> triggerRouters = triggerRouterService
                .getTriggerRouterForTableForCurrentNode(catalogName, schemaName, tableName, true);
        if (triggerRouters == null || triggerRouters.size() == 0) {
            return "Trigger for table " + tableName + " does not exist from node "
                    + sourceNode.getNodeGroupId();
        }

        insertSqlEvent(targetNode, triggerRouters.iterator().next().getTrigger(), sql, isLoad);

        return "Successfully create SQL event for node " + targetNode.getNodeId();
    }

    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName) {
        return reloadTable(nodeId, catalogName, schemaName, tableName, null);
    }

    public String reloadTable(String nodeId, String catalogName, String schemaName,
            String tableName, String overrideInitialLoadSelect) {
        Node sourceNode = nodeService.findIdentity();
        Node targetNode = nodeService.findNode(nodeId);
        if (targetNode == null) {

            return "Unknown node " + nodeId;
        }

        Set<TriggerRouter> triggerRouters = triggerRouterService
                .getTriggerRouterForTableForCurrentNode(catalogName, schemaName, tableName, true);
        if (triggerRouters == null || triggerRouters.size() == 0) {

            return "Trigger for table " + tableName + " does not exist from node "
                    + sourceNode.getNodeGroupId();
        }

        for (TriggerRouter triggerRouter : triggerRouters) {
            if (parameterService.is(ParameterConstants.INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD)) {
                String xml = symmetricDialect.getCreateTableXML(triggerRouter);
                insertCreateEvent(targetNode, triggerRouter, xml, true);
            } else if (parameterService.is(ParameterConstants.INITIAL_LOAD_DELETE_BEFORE_RELOAD)) {
                insertPurgeEvent(targetNode, triggerRouter, true);
            }

            insertReloadEvent(targetNode, triggerRouter, overrideInitialLoadSelect);
        }

        return "Successfully created event to reload table " + tableName + " for node "
                + targetNode.getNodeId();
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially
     * generating heartbeat events.
     * 
     * @param node
     */
    public void insertHeartbeatEvent(Node node, boolean isReload) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            String tableName = TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE);
            List<NodeGroupLink> links = configurationService.getNodeGroupLinksFor(parameterService
                    .getNodeGroupId());
            for (NodeGroupLink nodeGroupLink : links) {
                if (nodeGroupLink.getDataEventAction() == NodeGroupLinkAction.P) {
                    Set<TriggerRouter> triggerRouters = triggerRouterService
                            .getTriggerRouterForTableForCurrentNode(nodeGroupLink, null, null,
                                    tableName, false);
                    if (triggerRouters != null && triggerRouters.size() > 0) {
                        Data data = createData(transaction, triggerRouters.iterator().next()
                                .getTrigger(), String.format(" t.node_id = '%s'", node.getNodeId()));
                        if (data != null) {
                            insertData(transaction, data);
                        } else {
                            log.warn(
                                    "Not generating data/data events for table {} because a trigger or trigger hist is not created yet.",
                                    tableName);
                        }
                    } else {
                        log.warn(
                                "Not generating data/data events for table {} because a trigger or trigger hist is not created yet.",
                                tableName);
                    }
                }
            }
            transaction.commit();
        } finally {
            close(transaction);
        }

    }

    public Data createData(String catalogName, String schemaName, String tableName) {
        return createData(catalogName, schemaName, tableName, null);
    }

    public Data createData(String catalogName, String schemaName, String tableName,
            String whereClause) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            Data data = createData(transaction, catalogName, schemaName, tableName, whereClause);
            transaction.commit();
            return data;
        } finally {
            close(transaction);
        }
    }

    public Data createData(ISqlTransaction transaction, String catalogName, String schemaName,
            String tableName, String whereClause) {
        Data data = null;
        Set<TriggerRouter> triggerRouters = triggerRouterService
                .getTriggerRouterForTableForCurrentNode(catalogName, schemaName, tableName, false);
        if (triggerRouters != null && triggerRouters.size() > 0) {
            data = createData(transaction, triggerRouters.iterator().next().getTrigger(),
                    whereClause);
        }
        return data;
    }

    protected Data createData(ISqlTransaction transaction, Trigger trigger, String whereClause) {
        Data data = null;
        if (trigger != null) {
            TriggerHistory triggerHistory = triggerRouterService.getNewestTriggerHistoryForTrigger(
                    trigger.getTriggerId(), trigger.getSourceCatalogName(),
                    trigger.getSourceSchemaName(), trigger.getSourceTableName());
            if (triggerHistory == null) {
                triggerHistory = triggerRouterService.findTriggerHistory(trigger
                        .getSourceTableName());
                if (triggerHistory == null) {
                    triggerHistory = triggerRouterService.findTriggerHistory(trigger
                            .getSourceTableName().toUpperCase());
                }
            }
            if (triggerHistory != null) {
                String rowData = null;
                String pkData = null;
                if (whereClause != null) {
                    rowData = (String) transaction.queryForObject(symmetricDialect
                            .createCsvDataSql(trigger, triggerHistory,
                                    configurationService.getChannel(trigger.getChannelId()),
                                    whereClause), String.class);
                    if (rowData != null) {
                        rowData = rowData.trim();
                    }
                    pkData = (String) transaction.queryForObject(symmetricDialect
                            .createCsvPrimaryKeySql(trigger, triggerHistory,
                                    configurationService.getChannel(trigger.getChannelId()),
                                    whereClause), String.class);
                    if (pkData != null) {
                        pkData = pkData.trim();
                    }
                }
                data = new Data(trigger.getSourceTableName(), DataEventType.UPDATE, rowData,
                        pkData, triggerHistory, trigger.getChannelId(), null, null);
            }
        }
        return data;
    }

    public List<DataGap> findDataGapsByStatus(DataGap.Status status) {
        return sqlTemplate.query(getSql("findDataGapsByStatusSql"), new ISqlRowMapper<DataGap>() {
            public DataGap mapRow(Row rs) {
                return new DataGap(rs.getLong("start_id"), rs.getLong("end_id"), rs
                        .getDateTime("create_time"));
            }
        }, status.name());
    }

    public List<DataGap> findDataGaps() {
        final long maxDataToSelect = parameterService
                .getInt(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
        List<DataGap> gaps = findDataGapsByStatus(DataGap.Status.GP);
        boolean lastGapExists = false;
        for (DataGap dataGap : gaps) {
            lastGapExists |= dataGap.gapSize() >= maxDataToSelect - 1;
        }

        if (!lastGapExists) {
            long maxDataId = findMaxDataEventDataId();
            if (maxDataId > 0) {
                maxDataId++;
            }
            insertDataGap(new DataGap(maxDataId, maxDataId + maxDataToSelect));
            gaps = findDataGaps();
        }
        return gaps;

    }

    public long findMaxDataEventDataId() {
        return sqlTemplate.queryForLong(getSql("selectMaxDataEventDataIdSql"));
    }

    public void insertDataGap(DataGap gap) {
        try {
            sqlTemplate.update(getSql("insertDataGapSql"), new Object[] { DataGap.Status.GP.name(),
                    AppUtils.getHostName(), gap.getStartId(), gap.getEndId() }, new int[] {
                    Types.VARCHAR, Types.VARCHAR, Types.NUMERIC, Types.NUMERIC });
        } catch (UniqueKeyException ex) {
            log.warn("A gap already existed for {} to {}.  Updating instead.", gap.getStartId(),
                    gap.getEndId());
            updateDataGap(gap, DataGap.Status.GP);
        }
    }

    public void updateDataGap(DataGap gap, DataGap.Status status) {
        sqlTemplate.update(
                getSql("updateDataGapSql"),
                new Object[] { status.name(), AppUtils.getHostName(), gap.getStartId(),
                        gap.getEndId() }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.NUMERIC,
                        Types.NUMERIC });
    }

    public void deleteDataGap(DataGap gap) {
        sqlTemplate.update(getSql("deleteDataGapSql"),
                new Object[] { gap.getStartId(), gap.getEndId() }, new int[] { Types.NUMERIC,
                        Types.NUMERIC });

    }

    public Date findCreateTimeOfEvent(long dataId) {
        return sqlTemplate.queryForObject(getSql("findDataEventCreateTimeSql"), Date.class, dataId);
    }

    public Date findCreateTimeOfData(long dataId) {
        return sqlTemplate.queryForObject(getSql("findDataCreateTimeSql"), Date.class, dataId);
    }

    public Map<String, String> getRowDataAsMap(Data data) {
        Map<String, String> map = new HashMap<String, String>();
        String[] columnNames = CsvUtils.tokenizeCsvData(data.getTriggerHistory().getColumnNames());
        String[] columnData = CsvUtils.tokenizeCsvData(data.getRowData());
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i].toLowerCase(), columnData[i]);
        }
        return map;
    }

    public void setRowDataFromMap(Data data, Map<String, String> map) {
        String[] columnNames = CsvUtils.tokenizeCsvData(data.getTriggerHistory().getColumnNames());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        for (String columnName : columnNames) {
            try {
                writer.write(map.get(columnName.toLowerCase()), true);
            } catch (IOException e) {
            }
        }
        writer.close();
        data.setRowData(out.toString());
    }

    /**
     * Get a list of {@link IHeartbeatListener}s that are ready for a heartbeat
     * according to
     * {@link IHeartbeatListener#getTimeBetweenHeartbeatsInSeconds()}
     * 
     * @param force
     *            if true, then return the entire list of
     *            {@link IHeartbeatListener}s
     */
    protected List<IHeartbeatListener> getHeartbeatListeners(boolean force) {
        if (force) {
            return this.heartbeatListeners;
        } else {
            List<IHeartbeatListener> listeners = new ArrayList<IHeartbeatListener>();
            if (listeners != null) {
                long ts = System.currentTimeMillis();
                for (IHeartbeatListener iHeartbeatListener : this.heartbeatListeners) {
                    Long lastHeartbeatTimestamp = lastHeartbeatTimestamps.get(iHeartbeatListener);
                    if (lastHeartbeatTimestamp == null
                            || lastHeartbeatTimestamp <= ts
                                    - (iHeartbeatListener.getTimeBetweenHeartbeatsInSeconds() * 1000)) {
                        listeners.add(iHeartbeatListener);
                    }
                }
            }
            return listeners;
        }
    }

    protected void updateLastHeartbeatTime(List<IHeartbeatListener> listeners) {
        if (listeners != null) {
            Long ts = System.currentTimeMillis();
            for (IHeartbeatListener iHeartbeatListener : listeners) {
                lastHeartbeatTimestamps.put(iHeartbeatListener, ts);
            }
        }
    }

    /**
     * @see IDataService#heartbeat()
     */
    public void heartbeat(boolean force) {
        List<IHeartbeatListener> listeners = getHeartbeatListeners(force);
        if (listeners.size() > 0) {
            Node me = nodeService.findIdentity();
            if (me != null) {
                log.info("Updating time and version node info");
                Calendar now = Calendar.getInstance();
                now.set(Calendar.MILLISECOND, 0);
                me.setDeploymentType(deploymentType);
                me.setHeartbeatTime(now.getTime());
                me.setTimezoneOffset(AppUtils.getTimezoneOffset());
                me.setSymmetricVersion(Version.version());
                me.setDatabaseType(symmetricDialect.getName());
                me.setDatabaseVersion(symmetricDialect.getVersion());
                me.setBatchInErrorCount(outgoingBatchService.countOutgoingBatchesInError());
                if (parameterService.is(ParameterConstants.AUTO_UPDATE_NODE_VALUES)) {
                    log.info("Updating my node configuration info according to the symmetric properties");
                    me.setSchemaVersion(parameterService
                            .getString(ParameterConstants.SCHEMA_VERSION));
                    me.setExternalId(parameterService.getExternalId());
                    me.setNodeGroupId(parameterService.getNodeGroupId());
                    if (!StringUtils.isBlank(parameterService.getSyncUrl())) {
                        me.setSyncUrl(parameterService.getSyncUrl());
                    }
                }

                nodeService.save(me);
                nodeService.updateNodeHostForCurrentNode();
                log.info("Done updating my node info.");

                Set<Node> children = nodeService.findNodesThatOriginatedFromNodeId(me.getNodeId());
                for (IHeartbeatListener l : listeners) {
                    l.heartbeat(me, children);
                }

                updateLastHeartbeatTime(listeners);

            } else {
                log.debug("Did not run the heartbeat process because the node has not been configured");
            }
        }
    }

    public void setReloadListeners(List<IReloadListener> listeners) {
        this.reloadListeners = listeners;
    }

    public void addReloadListener(IReloadListener listener) {
        if (reloadListeners == null) {
            reloadListeners = new ArrayList<IReloadListener>();
        }
        reloadListeners.add(listener);
    }

    public boolean removeReloadListener(IReloadListener listener) {
        if (reloadListeners != null) {
            return reloadListeners.remove(listener);
        } else {
            return false;
        }
    }

    public void setHeartbeatListeners(List<IHeartbeatListener> listeners) {
        this.heartbeatListeners = listeners;
    }

    public void addHeartbeatListener(IHeartbeatListener listener) {
        if (heartbeatListeners == null) {
            heartbeatListeners = new ArrayList<IHeartbeatListener>();
        }
        heartbeatListeners.add(listener);
    }

    public boolean removeHeartbeatListener(IHeartbeatListener listener) {
        if (heartbeatListeners != null) {
            return heartbeatListeners.remove(listener);
        } else {
            return false;
        }
    }

    public List<Number> listDataIds(long batchId, String nodeId) {
        return sqlTemplate.query(getSql("selectEventDataIdsSql", " order by d.data_id asc"),
                new NumberMapper(), batchId, nodeId);
    }

    public List<Data> listData(long batchId, String nodeId, long startDataId, String channelId,
            final int maxRowsToRetrieve) {
        return sqlTemplate.query(getDataSelectSql(batchId, startDataId, channelId),
                maxRowsToRetrieve, this.dataMapper, batchId, nodeId, startDataId);
    }

    public Data mapData(Row row) {
        return dataMapper.mapRow(row);
    }

    public ISqlReadCursor<Data> selectDataFor(Batch batch) {
        return sqlTemplate
                .queryForCursor(getDataSelectSql(batch.getBatchId(), -1l, batch.getChannelId()),
                        dataMapper, new Object[] { batch.getBatchId(), batch.getTargetNodeId() },
                        new int[] { Types.NUMERIC });
    }

    protected String getDataSelectSql(long batchId, long startDataId, String channelId) {
        String startAtDataIdSql = startDataId >= 0l ? " and d.data_id >= ? " : "";
        return symmetricDialect.massageDataExtractionSql(
                getSql("selectEventDataToExtractSql", startAtDataIdSql, " order by d.data_id asc"),
                configurationService.getNodeChannel(channelId, false).getChannel());
    }

    public long findMaxDataId() {
        return sqlTemplate.queryForLong(getSql("selectMaxDataIdSql"));
    }

    public class DataMapper implements ISqlRowMapper<Data> {
        public Data mapRow(Row row) {
            Data data = new Data();
            data.putCsvData(CsvData.ROW_DATA, row.getString("ROW_DATA", false));
            data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA", false));
            data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA", false));
            data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_NAME, row.getString("TABLE_NAME"));
            data.setDataEventType(DataEventType.getEventType(row.getString("EVENT_TYPE")));
            data.putAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID, row.getString("SOURCE_NODE_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_EXTERNAL_DATA, row.getString("EXTERNAL_DATA"));
            data.putAttribute(CsvData.ATTRIBUTE_DATA_ID, row.getLong("DATA_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_CREATE_TIME, row.getDateTime("CREATE_TIME"));
            data.putAttribute(CsvData.ATTRIBUTE_ROUTER_ID, row.getString("ROUTER_ID", false));
            int triggerHistId = row.getInt("TRIGGER_HIST_ID");
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_ID, triggerHistId);
            data.setTriggerHistory(triggerRouterService.getTriggerHistory(triggerHistId));
            if (data.getTriggerHistory() == null) {
                data.setTriggerHistory(new TriggerHistory(triggerHistId));
            }
            return data;
        }
    }

}
