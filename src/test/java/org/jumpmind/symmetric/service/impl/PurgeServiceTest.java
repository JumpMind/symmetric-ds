/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.util.Calendar;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;
import org.mortbay.log.Log;

public class PurgeServiceTest extends AbstractDatabaseTest {

    public PurgeServiceTest() throws Exception {
        super();
    }
    
    @Test
    public void testThatPurgeExecutes() {
        IPurgeService service = find(Constants.PURGE_SERVICE);
        service.purge();
    }

    @Test
    public void testThatPurgeDoesNotDeleteSuccessfullySentData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupSentData();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(1);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }
    
    @Test
    public void testThatPurgeDeletesSuccessfullySentData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupSentData();
        makeDataOld();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }    

    private void setupSentData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, TestConstants.TEST_CLIENT_EXTERNAL_ID,
                router.getRouter().getRouterId());
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
    }
    
    @Test
    public void testThatPurgeDeletesSuccessfullyIgnoredData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupIgnoredData();
        makeDataOld();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    } 
    
    private void setupIgnoredData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, TestConstants.TEST_CLIENT_EXTERNAL_ID,
                router.getRouter().getRouterId());
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
        getJdbcTemplate().update("update sym_outgoing_batch set status=?", new Object[] { Status.IG.name() });
    }

    @Test
    public void testThatPurgeDoesNotDeleteNewData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupNewData();
        assertCounts(1, 0, 0);
        getSymmetricEngine().purge();
        assertCounts(1, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }
    
    @Test
    public void testThatPurgeDoesNotDeleteNewDataThatIsOld() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupNewData();
        makeDataOld();
        assertCounts(1, 0, 0);
        getSymmetricEngine().purge();
        assertCounts(1, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }

    private void setupNewData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertData(data);
        if (getJdbcTemplate().update("update sym_data_ref set ref_data_id=(select max(data_id)-1 from sym_data)") == 0)  {
            Log.info("Inserting into sym_data_ref");
            getJdbcTemplate().update("insert into sym_data_ref values(1, current_timestamp)");
        }
    }

    @Test
    public void testThatPurgeDoesNotDeleteUnroutedData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupUnroutedData();
        getSymmetricEngine().purge();
        assertCounts(1, 1, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }
    
    @Test
    public void testThatPurgeDeletesUnroutedData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupUnroutedData();
        makeDataOld();
        assertCounts(1, 1, 0);
        getSymmetricEngine().purge();
        assertCounts(0, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }    

    private void setupUnroutedData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertData(data);
        getDataService().insertDataEvent(data.getDataId(), Constants.UNROUTED_BATCH_ID, Constants.UNKNOWN_ROUTER_ID);
    }
        
    @Test
    public void testThatPurgeDoesNotDeletePartiallySentData() {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupPartiallySentData();
        makeDataOld();
        assertCounts(1, 3, 3);
        getSymmetricEngine().purge();
        assertCounts(1, 2, 2);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }
    
    @Test
    public void testThatPurgeRemovesDataForNodesThatHaveBeenDisabled() {
        int oldPurgeRetentionPeriod = getParameterService().getInt(ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupDataForDisabledNode();
        makeDataOld();
        assertCounts(1, 1, 1);
        getSymmetricEngine().purge();
        assertCounts(0, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, oldPurgeRetentionPeriod);
    }
    
    private void setupPartiallySentData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, TestConstants.TEST_CLIENT_EXTERNAL_ID,
                router.getRouter().getRouterId());
        int dataId = getJdbcTemplate().queryForInt("select max(data_id) from sym_data");
        getDataService().insertDataEventAndOutgoingBatch(dataId, data.getChannelId(), "00003", router.getRouter().getRouterId());
        getDataService().insertDataEventAndOutgoingBatch(dataId, data.getChannelId(), "00010", router.getRouter().getRouterId());
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
    }
    
    private void setupDataForDisabledNode() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false).values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "", "",
                getTriggerRouterService().getNewestTriggerHistoryForTrigger(router.getTrigger().getTriggerId()),
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, "00002",
                router.getRouter().getRouterId());
    }
    
    private void assertCounts(int count) {
        assertCounts(count, count, count);
    }

    private void assertCounts(int dataCount, int dataEventCount, int batchCount) {
        assertNumberOfRows(dataCount, "sym_data");
        assertNumberOfRows(dataEventCount, "sym_data_event");
        assertNumberOfRows(batchCount, "sym_outgoing_batch");
    }

    private void makeDataOld() {
        Calendar oldTime = Calendar.getInstance();
        oldTime.add(Calendar.DATE, -1);
        getJdbcTemplate().update("update sym_data set create_time=?", new Object[] { oldTime });
        getJdbcTemplate().update("update sym_data_event set create_time=?", new Object[] { oldTime });
        getJdbcTemplate().update("update sym_outgoing_batch set create_time=?", new Object[] { oldTime });
    }

}
