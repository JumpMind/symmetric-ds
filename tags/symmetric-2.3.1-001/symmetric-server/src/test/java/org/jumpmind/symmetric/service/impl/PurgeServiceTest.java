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
 * under the License.  */
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

/**
 * 
 */
public class PurgeServiceTest extends AbstractDatabaseTest {

    public PurgeServiceTest() throws Exception {
        super();
    }

    @Test
    public void testThatPurgeExecutes() {
        IPurgeService service = find(Constants.PURGE_SERVICE);
        service.purgeOutgoing();
    }

    @Test
    public void testThatPurgeDoesNotDeleteSuccessfullySentData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupSentData();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(1);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    @Test
    public void testThatPurgeDeletesSuccessfullySentData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupSentData();
        makeDataOld();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(1);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    private void setupSentData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, router.getRouter().getRouterId(), false);
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
    }

    @Test
    public void testThatPurgeDeletesSuccessfullyIgnoredData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupIgnoredData();
        makeDataOld();
        assertCounts(1);
        getSymmetricEngine().purge();
        assertCounts(1);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    private void setupIgnoredData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, router.getRouter().getRouterId(), false);
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
        getJdbcTemplate().update("update sym_outgoing_batch set status=?",
                new Object[] { Status.IG.name() });
    }

    @Test
    public void testThatPurgeDoesNotDeleteNewData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupNewData();
        assertCounts(1, 0, 0);
        getSymmetricEngine().purge();
        assertCounts(1, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    @Test
    public void testThatPurgeDoesNotDeleteNewDataThatIsOld() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupNewData();
        makeDataOld();
        assertCounts(1, 0, 0);
        getSymmetricEngine().purge();
        assertCounts(1, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    private void setupNewData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        data.setDataId(1);
        getDataService().insertData(data);
        if (getJdbcTemplate().update(
                "update sym_data_ref set ref_data_id=(select max(data_id)-1 from sym_data)") == 0) {
            getJdbcTemplate().update(getDbDialect().scrubSql("insert into sym_data_ref values(1, current_timestamp)"));
        }
    }

    @Test
    public void testThatPurgeDoesNotDeleteUnroutedData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupUnroutedData(1);
        getSymmetricEngine().purge();
        assertCounts(1, 1, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    @Test
    public void testThatPurgeDeletesUnroutedData() throws Exception {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupUnroutedData(1, 2);
        makeDataOld();
        assertCounts(2, 2, 0);
        getSymmetricEngine().purge();
        assertCounts(1, 0, 0);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    private void setupUnroutedData(int... id) {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        if (id != null) {
            for (int i : id) {
                data.setDataId(i);
                getDataService().insertData(data);
                getDataService().insertDataEvent(data.getDataId(), Constants.UNROUTED_BATCH_ID,
                        Constants.UNKNOWN_ROUTER_ID);
            }
        }
    }

    @Test
    public void testThatPurgeDoesNotDeletePartiallySentData() {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupPartiallySentData();
        makeDataOld();
        assertCounts(1, 3, 3);
        getSymmetricEngine().purge();
        assertCounts(1, 2, 2);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    @Test
    public void testThatPurgeRemovesDataForNodesThatHaveBeenDisabled() {
        int oldPurgeRetentionPeriod = getParameterService().getInt(
                ParameterConstants.PURGE_RETENTION_MINUTES);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES, 10);
        setupDataForDisabledNode();
        makeDataOld();
        assertCounts(2, 2, 2);
        getSymmetricEngine().purge();
        assertCounts(1, 1, 1);
        getParameterService().saveParameter(ParameterConstants.PURGE_RETENTION_MINUTES,
                oldPurgeRetentionPeriod);
    }

    private void setupPartiallySentData() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data,
                TestConstants.TEST_CLIENT_EXTERNAL_ID, router.getRouter().getRouterId(), false);
        int dataId = getJdbcTemplate().queryForInt("select max(data_id) from sym_data");
        data.setDataId(dataId);
        getDataService().insertDataEventAndOutgoingBatch(dataId, data.getChannelId(), "00003", DataEventType.INSERT,
                router.getRouter().getRouterId(), false);
        getDataService().insertDataEventAndOutgoingBatch(dataId, data.getChannelId(), "00010", DataEventType.INSERT,
                router.getRouter().getRouterId(), false);
        getOutgoingBatchService().markAllAsSentForNode(TestConstants.TEST_CLIENT_NODE);
    }

    private void setupDataForDisabledNode() {
        cleanSlate("sym_data", "sym_data_event", "sym_outgoing_batch", "sym_incoming_batch");
        assertCounts(0);
        TriggerRouter router = getTriggerRouterService().getTriggerRoutersForCurrentNode(false)
                .values().iterator().next().get(0);
        Data data = new Data(router.getTrigger().getSourceTableName(), DataEventType.INSERT, "",
                "", getTriggerRouterService().getNewestTriggerHistoryForTrigger(
                        router.getTrigger().getTriggerId()), TestConstants.TEST_CHANNEL_ID, null,
                null);
        data.setDataId(1);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, "00002",
                router.getRouter().getRouterId(), false);
        data.setDataId(2);
        getDataService().insertDataAndDataEventAndOutgoingBatch(data, "00002",
                router.getRouter().getRouterId(), false);        
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
        getJdbcTemplate().update("update sym_data_event set create_time=?",
                new Object[] { oldTime });
        getJdbcTemplate().update("update sym_outgoing_batch set create_time=?",
                new Object[] { oldTime });
    }

}