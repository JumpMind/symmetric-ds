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
package org.jumpmind.symmetric.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.load.IReloadGenerator;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TableReloadStatus;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IGroupletService;
import org.jumpmind.symmetric.service.IInitialLoadService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentMatchers;

public class DataServiceTest {
    ISqlTemplate sqlTemplate;
    ISqlTransaction sqlTransaction;
    IDataService dataService;
    IParameterService parameterService;
    ISymmetricDialect symmetricDialect;
    TableReloadRequest request;
    ISymmetricEngine engine;
    IDatabasePlatform platform;

    @BeforeEach
    public void setUp() throws Exception {
        sqlTemplate = mock(ISqlTemplate.class);
        sqlTransaction = mock(ISqlTransaction.class);
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        platform = mock(IDatabasePlatform.class);
        when(platform.getDatabaseInfo()).thenReturn(new DatabaseInfo());
        when(platform.getSqlTemplate()).thenReturn(sqlTemplate);
        symmetricDialect = mock(AbstractSymmetricDialect.class);
        when(symmetricDialect.getPlatform()).thenReturn(platform);
        parameterService = mock(ParameterService.class);
        when(parameterService.getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE)).thenReturn(50000000L);
        IExtensionService extensionService = mock(ExtensionService.class);
        engine = mock(AbstractSymmetricEngine.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(engine.getSymmetricDialect()).thenReturn(symmetricDialect);
        when(parameterService.getTablePrefix()).thenReturn("sym");
        when(platform.getSqlTemplateDirty()).thenReturn(sqlTemplate);
        dataService = new DataService(engine, extensionService);
        // request = mock(TableReloadRequest.class);
    }

    @Test
    public void testFindDataGaps2() throws Exception {
        final List<DataGap> gaps1 = new ArrayList<DataGap>();
        gaps1.add(new DataGap(30953884, 80953883));
        gaps1.add(new DataGap(30953883, 80953883));
        when(sqlTemplate.queryForLong(ArgumentMatchers.anyString())).thenReturn(0L);
        String sql = ArgumentMatchers.anyString();
        @SuppressWarnings("unchecked")
        ISqlRowMapper<DataGap> anyMapper = (ISqlRowMapper<DataGap>) ArgumentMatchers.any();
        when(sqlTemplate.query(sql, anyMapper, (Object[]) ArgumentMatchers.any())).thenReturn(gaps1);
        dataService.findDataGaps();
        verifyNoMoreInteractions(sqlTransaction);
    }

    @Test
    void testInsertTableReloadRequest() throws Exception {
        // mocked interactions
        request = new TableReloadRequest();
        when(engine.getDatabasePlatform()).thenReturn(platform);
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        when(sqlTransaction.prepareAndExecute(ArgumentMatchers.anyString(), (JdbcSqlTransaction) ArgumentMatchers.any())).thenReturn(1);
        dataService.insertTableReloadRequest(request);
    }

    @ParameterizedTest
    @CsvSource({ "" + 0 + "", "" + 1 + "", "" + 2 + "" })
    void testInsertReloadEvents(int scenario) throws Exception {
        // actual variables
        Node targetNode = new Node();
        targetNode.setNodeGroupId("client");
        targetNode.setExternalId("client");
        targetNode.setNodeId("client");
        Node sourceNode = new Node();
        sourceNode.setExternalId("server");
        sourceNode.setNodeGroupId("server");
        sourceNode.setNodeId("server");
        NodeGroupLink link = new NodeGroupLink("server", "client");
        List<Channel> channels = new ArrayList<Channel>();
        Trigger trigger = null;
        if (scenario == 2) {
            trigger = new Trigger("sym_file_snapshot", "default");
        } else {
            trigger = new Trigger("testTable", "default");
        }
        Router router = new Router("testRouter", link);
        TriggerRouter triggerRouter = new TriggerRouter(trigger, router);
        request = new TableReloadRequest();
        TableReloadRequest reloadRequest = new TableReloadRequest();
        reloadRequest.setLoadId((long) 1);
        reloadRequest.setTriggerId("testTable");
        reloadRequest.setRouterId("testRouter");
        TableReloadRequest reloadRequestForAll = new TableReloadRequest();
        reloadRequestForAll.setLoadId((long) 1);
        reloadRequestForAll.setTriggerId("ALL");
        reloadRequestForAll.setRouterId("ALL");
        Table table = new Table("testTable");
        List<String> columns = new ArrayList<String>();
        columns.add("Id");
        columns.add("age");
        columns.add("weight");
        int counter = 0;
        for (String columnName : columns) {
            Column column = new Column(columnName);
            if (counter < 1) {
                column.setPrimaryKey(true);
            }
            table.addColumn(column);
            counter++;
        }
        TableReloadStatus tableReloadStatus = new TableReloadStatus();
        List<TableReloadRequest> reloadRequests = new ArrayList<TableReloadRequest>();
        if (scenario == 2) {
            TableReloadRequest fileSyncReloadRequest = new TableReloadRequest();
            fileSyncReloadRequest.setLoadId((long) 1);
            fileSyncReloadRequest.setTriggerId("sym_file_snapshot");
            fileSyncReloadRequest.setRouterId("testRouter");
            reloadRequests.add(fileSyncReloadRequest);
           
            Channel channel = new Channel("filesync", 0, 1000, 1000, true,
                    (long) 9999999, false, true, true);
            channels.add(channel);
        } else {
            reloadRequests.add(reloadRequest);
        }
        List<TableReloadRequest> reloadRequestsForAll = new ArrayList<TableReloadRequest>();
        reloadRequestsForAll.add(reloadRequestForAll);
        ProcessInfo processInfo = new ProcessInfo();
        List<TriggerRouter> triggerRouters = new ArrayList<TriggerRouter>();
        triggerRouters.add(triggerRouter);
        Map<Integer, ExtractRequest> extractRequests = new HashMap<Integer, ExtractRequest>();
        ExtractRequest extractRequest = new ExtractRequest();
        extractRequest.setCreateTime(null);
        extractRequest.setEndBatchId(2l);
        extractRequest.setExtractedMillis(10000);
        extractRequest.setExtractedRows(200);
        extractRequest.setLastLoadedBatchId(2);
        extractRequest.setLastTransferredBatchId(2);
        extractRequest.setLastUpdateTime(null);
        extractRequest.setLoadedMillis(30000);
        extractRequest.setLoadedRows(5);
        extractRequest.setLoadId(0);
        extractRequest.setNodeId("server");
        extractRequest.setParentRequestId(0);
        extractRequest.setQueue(null);
        extractRequest.setRequestId(1);
        extractRequest.setRouterId(null);
        extractRequest.setRows(20000);
        extractRequest.setStartBatchId(0);
        extractRequest.setStatus(null);
        extractRequest.setTableName(null);
        extractRequest.setTransferredMillis(2000);
        extractRequest.setTransferredRows(400);
        extractRequest.setTriggerId(null);
        extractRequest.setTriggerRouter(triggerRouter);
        extractRequests.put(0, extractRequest);
        Map<Integer, List<TriggerRouter>> triggerRouterByHist = new HashMap<Integer, List<TriggerRouter>>();
        triggerRouterByHist.put(0, triggerRouters);
        List<TriggerHistory> triggerHistories = new ArrayList<TriggerHistory>();
        TriggerHistory triggerHistory = new TriggerHistory("testTable", "Id", "Id,age,weight");
        triggerHistory.setTriggerId("testTable");
        triggerHistories.add(triggerHistory);
        Map<String, Channel> channelMap = new HashMap<String, Channel>();
        Channel channel = new Channel("default", 0);
        channelMap.put("default", channel);
        Set<TriggerRouter> triggerRouterSet = new HashSet<TriggerRouter>();
        Trigger triggerForSet = new Trigger("sym_node_security", "default");
        Router routerForSet = new Router("routerForSet", link);
        TriggerRouter triggerRouterForSet = new TriggerRouter(triggerForSet, routerForSet);
        TriggerHistory triggerHist = new TriggerHistory("sym_node_security", "NODE_ID",
                "NODE_ID,NODE_PASSWORD,REGISTRATION_ENABLED,REGISTRATION_TIME,REGISTRATION_NOT_BEFORE,REGISTRATION_NOT_AFTER,INITIAL_LOAD_ENABLED,INITIAL_LOAD_TIME,INITIAL_LOAD_END_TIME,INITIAL_LOAD_ID,INITIAL_LOAD_CREATE_BY,REV_INITIAL_LOAD_ENABLED,REV_INITIAL_LOAD_TIME,REV_INITIAL_LOAD_ID,REV_INITIAL_LOAD_CREATE_BY,FAILED_LOGINS,CREATED_AT_NODE_ID");
        triggerRouterSet.add(triggerRouterForSet);
        IReloadGenerator reloadGenerator = mock(IReloadGenerator.class);
        IClusterService clusterService = mock(IClusterService.class);
        INodeService nodeService = mock(INodeService.class);
        TriggerRouterService triggerRouterService = mock(TriggerRouterService.class);
        IInitialLoadService initialLoadService = mock(IInitialLoadService.class);
        NodeSecurity nodeSecurity = mock(NodeSecurity.class);
        ISequenceService sequenceService = mock(ISequenceService.class);
        IDataExtractorService dataExtractorService = mock(IDataExtractorService.class);
        IConfigurationService configurationService = mock(IConfigurationService.class);
        IGroupletService groupletService = mock(IGroupletService.class);
        ITransformService transformService = mock(ITransformService.class);
        IOutgoingBatchService outgoingBatchService = mock(IOutgoingBatchService.class);
        IStatisticManager statisticManager = mock(IStatisticManager.class);
        IPurgeService purgeService = mock(IPurgeService.class);
        IFileSyncService fileSyncService = mock(IFileSyncService.class);
        // mocked interactions
        when(engine.getClusterService()).thenReturn(clusterService);
        when(clusterService.lock(ClusterConstants.SYNC_TRIGGERS)).thenReturn(true);
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(sourceNode);
        when(nodeService.findNodeSecurity(ArgumentMatchers.anyString())).thenReturn(nodeSecurity);
        when(engine.getTriggerRouterService()).thenReturn(triggerRouterService);
        when(parameterService.is(ParameterConstants.DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL)).thenReturn(true);
        when(engine.getNodeId()).thenReturn("server");
        when(engine.getInitialLoadService()).thenReturn(initialLoadService);
        doNothing().when(initialLoadService).cancelLoad(ArgumentMatchers.any());
        when(engine.getDatabasePlatform()).thenReturn(platform);
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        when(sqlTransaction.prepareAndExecute(ArgumentMatchers.anyString(), (JdbcSqlTransaction) ArgumentMatchers.any())).thenReturn(1);
        when(reloadGenerator.getActiveTriggerHistories(targetNode)).thenReturn(triggerHistories);
        when(platform.getSqlTemplate()).thenReturn(sqlTemplate);
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        when(platform.supportsMultiThreadedTransactions()).thenReturn(false);
        when(engine.getSequenceService()).thenReturn(sequenceService);
        when(sequenceService.nextVal(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1L);
        when(nodeSecurity.getInitialLoadCreateBy()).thenReturn("test user");
        when(triggerRouterService.getActiveTriggerHistories((Trigger) ArgumentMatchers.any())).thenReturn(triggerHistories);
        when(triggerRouterService.fillTriggerRoutersByHistIdAndSortHist(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyList(), ArgumentMatchers.anyList())).thenReturn(
                        triggerRouterByHist);
        when(engine.getConfigurationService()).thenReturn(configurationService);
        when(configurationService.getChannels(false)).thenReturn(channelMap);
        when(engine.getConfigurationService().getChannels(false)).thenReturn(channelMap);
        when(engine.getGroupletService()).thenReturn(groupletService);
        when(groupletService.isTargetEnabled(triggerRouter, targetNode)).thenReturn(true);
        when(engine.getDataExtractorService()).thenReturn(dataExtractorService);
        when(symmetricDialect.getTargetDialect()).thenReturn(symmetricDialect);
        when(platform.getTableFromCache(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyBoolean())).thenReturn(
                table);
        doNothing().when(dataExtractorService).releaseMissedExtractRequests();
        when(triggerRouterService.getRouterById(ArgumentMatchers.anyString(), ArgumentMatchers.anyBoolean())).thenReturn(router);
        when(engine.getTransformService()).thenReturn(transformService);
        when(transformService.findTransformsFor(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(null);
        when(engine.getOutgoingBatchService()).thenReturn(outgoingBatchService);
        doNothing().when(outgoingBatchService).insertOutgoingBatch(ArgumentMatchers.any(), ArgumentMatchers.any());
        when(engine.getStatisticManager()).thenReturn(statisticManager);
        doNothing().when(statisticManager).incrementNodesLoaded(1);
        when(sqlTransaction.prepareAndExecute(ArgumentMatchers.anyString(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1);
        when(sqlTransaction.prepareAndExecute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(1);
        when(sqlTemplate.queryForObject(ArgumentMatchers.anyString(), (ISqlRowMapper<TableReloadStatus>) ArgumentMatchers.any(), ArgumentMatchers.anyLong(),
                ArgumentMatchers.anyString())).thenReturn(tableReloadStatus);
        when(dataExtractorService.requestExtractRequest(sqlTransaction, targetNode.getNodeId(), channel.getQueue(), triggerRouter, -1, -1, 1, table.getName(),
                0, 0)).thenReturn(extractRequest);
        // Scenario 1 callers for Full Load testing
        if (scenario == 1) {
            doNothing().when(outgoingBatchService).markAllAsSentForNode(targetNode.getNodeId(), false);
            doReturn(triggerRouterSet).when(triggerRouterService).getTriggerRouterForTableForCurrentNode(ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.anyString(), ArgumentMatchers.anyBoolean());
            doReturn(triggerHist).when(triggerRouterService).getNewestTriggerHistoryForTrigger(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers
                    .any(), ArgumentMatchers.any());
            when(engine.getTriggerRouterService()
                    .findTriggerHistoryForGenericSync()).thenReturn(triggerHistory);
            when(engine.getTriggerRouterService().getTriggerById("testTable", false)).thenReturn(trigger);
            when(engine.getPurgeService()).thenReturn(purgeService);
            doNothing().when(purgeService).purgeAllIncomingEventsForNode(ArgumentMatchers.anyString());
        }
        // Scenario 2 callers for Filesync testing
        if (scenario == 2) {
            List<FileTriggerRouter> fileTriggerRouters = new ArrayList<FileTriggerRouter>();
            FileTrigger fileTrigger = new FileTrigger("basedir/test", true, "*", null);
            FileTriggerRouter fileTriggerRouter = new FileTriggerRouter(fileTrigger, router);
            Trigger fileSyncTrigger = new Trigger("sym_file_snapshot", "default");
            TriggerHistory fileSyncTriggerHist = new TriggerHistory("sym_file_snapshot", "trigger_id,router_id,relative_dir,file_name",
                    "trigger_id,router_id,relative_dir,file_name,channel_id,reload_channel_id,last_event_type,crc32_checksum,file_size,file_modified_time,last_update_time,last_update_by,create_time");
            fileSyncTriggerHist.setTriggerId("sym_file_snapshot");
            String routerName = String.format("%s_%s_2_%s", fileSyncTriggerHist.getTriggerId(), "server", targetNode.getNodeGroupId());
            TriggerRouter fileSyncTriggerRouter = new TriggerRouter(fileSyncTrigger, router);
            fileTriggerRouters.add(fileTriggerRouter);
            when(parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)).thenReturn(true);
            when(engine.getFileSyncService()).thenReturn(fileSyncService);
            when(fileSyncService.getFileTriggerRoutersForCurrentNode(false)).thenReturn(fileTriggerRouters);
            when(triggerRouterService.findTriggerHistory(null, null, "sym_file_snapshot")).thenReturn(fileSyncTriggerHist);
            when(parameterService.getNodeGroupId()).thenReturn("server");
            when(triggerRouterService.buildSymmetricTableRouterId(
                    fileSyncTriggerHist.getTriggerId(), "server",
                    targetNode.getNodeGroupId())).thenReturn(routerName);
            when(triggerRouterService.getTriggerRouterForCurrentNode(fileSyncTriggerHist.getTriggerId(),
                    routerName, true)).thenReturn(fileSyncTriggerRouter);
            when(engine.getConfigurationService()).thenReturn(configurationService);
            when(configurationService.getFileSyncChannels()).thenReturn(channels);
        }
        // Actual Tests and Results
        Map<Integer, ExtractRequest> actualResults = new HashMap<Integer, ExtractRequest>();
        Map<Integer, ExtractRequest> expectedResults = new HashMap<Integer, ExtractRequest>();
        if (scenario == 1) {
            actualResults = dataService.insertReloadEvents(targetNode, false, reloadRequestsForAll, processInfo, triggerRouters, extractRequests,
                    reloadGenerator);
        } else {
            actualResults = dataService.insertReloadEvents(targetNode, false, reloadRequests, processInfo, triggerRouters, extractRequests, reloadGenerator);
        }
        expectedResults.put(0, extractRequest);
        assertEquals(actualResults, expectedResults);
    }

    @ParameterizedTest
    @CsvSource({ "" + 0 + "", "" + 1 + "", "" + 2 + "" })
    void testSendSQL(int scenario) throws Exception {
        // actual variables
        Node targetNode = new Node();
        targetNode.setNodeGroupId("client");
        targetNode.setExternalId("client");
        targetNode.setNodeId("client");
        Node sourceNode = new Node();
        sourceNode.setExternalId("server");
        sourceNode.setNodeGroupId("server");
        sourceNode.setNodeId("server");
        TriggerHistory triggerHistory = new TriggerHistory("testTable", "Id", "Id,age,weight");
        triggerHistory.setTriggerId("testTable");
        NodeGroupLink link = new NodeGroupLink("server", "client");
        Trigger trigger = new Trigger("testTable", "default");
        Router router = new Router("testRouter", link);
        TriggerHistory triggerHist = new TriggerHistory("sym_node_host", "node_id,host_name",
                "node_id,host_name,instance_id,ip_address,os_user,os_name,os_arch,os_version,available_processors,free_memory_bytes,total_memory_bytes,max_memory_bytes,java_version,java_vendor,jdbc_version,symmetric_version,timezone_offset,heartbeat_time,last_restart_time,create_time");
        triggerHist.setTriggerId("sym_node_host");
        Trigger triggerForNodeHost = new Trigger("sym_node_host", "default");
        // mocked variables
        INodeService nodeService = mock(INodeService.class);
        ITriggerRouterService triggerRouterService = mock(ITriggerRouterService.class);
        IOutgoingBatchService outgoingBatchService = mock(IOutgoingBatchService.class);
        // mocked interactions
        when(parameterService.getTablePrefix()).thenReturn("sym");
        when(engine.getNodeService()).thenReturn(nodeService);
        when(nodeService.findIdentity()).thenReturn(sourceNode);
        when(nodeService.findNode(ArgumentMatchers.anyString(), ArgumentMatchers.anyBoolean())).thenReturn(targetNode);
        when(engine.getTriggerRouterService()).thenReturn(triggerRouterService);
        if (scenario == 2) {
            when(triggerRouterService.findTriggerHistory(null, null, "sym_node_host")).thenReturn(null);
        } else {
            when(triggerRouterService.findTriggerHistory(null, null, "sym_node_host")).thenReturn(triggerHist);
        }
        when(triggerRouterService.getTriggerById(triggerHist.getTriggerId())).thenReturn(triggerForNodeHost);
        when(sqlTemplate.startSqlTransaction()).thenReturn(sqlTransaction);
        when(engine.getOutgoingBatchService()).thenReturn(outgoingBatchService);
        doNothing().when(outgoingBatchService).insertOutgoingBatch(ArgumentMatchers.any(), ArgumentMatchers.any());
        // Actual Tests and Results
        if (scenario == 0) {
            dataService.sendSQL("client", null);
        } else if (scenario == 1) {
            dataService.sendSQL("failure", null);
        } else if (scenario == 2) {
            dataService.sendSQL("client", null);
        }
    }

    @Test
    void testGetTableReloadRequest() throws Exception {
        // actual variables
        TableReloadRequest reloadRequest = new TableReloadRequest();
        reloadRequest.setLoadId((long) 1);
        reloadRequest.setTriggerId("testTable");
        reloadRequest.setRouterId("testRouter");
        List<TableReloadRequest> reloadRequests = new ArrayList<TableReloadRequest>();
        reloadRequests.add(reloadRequest);
        // mocked interactions
        when(sqlTemplate.query(ArgumentMatchers.any(), (ISqlRowMapper<TableReloadRequest>) ArgumentMatchers.any(), ArgumentMatchers.anyLong())).thenReturn(
                reloadRequests);
        dataService.getTableReloadRequest((long) 0);
    }
}
