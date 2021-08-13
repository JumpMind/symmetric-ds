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
package org.jumpmind.symmetric.test;

import java.sql.Types;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.web.rest.NotAllowedException;
import org.jumpmind.symmetric.web.rest.RestService;
import org.jumpmind.symmetric.web.rest.model.Batch;
import org.jumpmind.symmetric.web.rest.model.BatchResult;
import org.jumpmind.symmetric.web.rest.model.BatchResults;
import org.jumpmind.symmetric.web.rest.model.PullDataResults;
import org.jumpmind.symmetric.web.rest.model.RegistrationInfo;
import org.jumpmind.util.FormatUtils;
import static org.junit.Assert.*;

public class RestServiceTest extends AbstractTest {
    @Override
    protected Table[] getTables(String name) {
        Table a = new Table("a");
        a.addColumn(new Column("id", true, Types.INTEGER, -1, -1));
        a.addColumn(new Column("notes", false, Types.VARCHAR, 255, -1));
        a.addColumn(new Column("created", false, Types.TIMESTAMP, -1, -1));
        return new Table[] { a };
    }

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        testRestPullApi();
    }

    protected void testRestPullApi() throws Exception {
        loadConfigAtRegistrationServer();
        RestService restService = getRegServer().getRestService();
        ISymmetricEngine engine = getRegServer().getEngine();
        IParameterService parameterService = engine.getParameterService();
        INodeService nodeService = engine.getNodeService();
        parameterService.saveParameter(ParameterConstants.REST_API_ENABLED, Boolean.TRUE,
                "unit_test");
        assertNotNull("Could not find the rest service in the application context",
                restService);
        List<Node> nodes = nodeService.findAllNodes();
        assertEquals("Expected there to only be one node registered", 1, nodes.size());
        assertEquals("The only node we expected to be registered is a server node",
                "server", nodes.get(0).getNodeGroupId());
        RegistrationInfo registrationInfo = restService.postRegisterNode("client", "client",
                DatabaseNamesConstants.SQLITE, "3.0", "hostName");
        assertNotNull("Registration should have returned a result object", registrationInfo);
        assertFalse("Registration should not have been open",
                registrationInfo.isRegistered());
        assertEquals("Expected there to only be one node registered", 1, nodes.size());
        engine.openRegistration("client", "client");
        registrationInfo = restService.postRegisterNode("client", "client",
                DatabaseNamesConstants.SQLITE, "3.0", "hostName");
        assertNotNull("Registration should have returned a result object", registrationInfo);
        assertTrue("Registration should have been open", registrationInfo.isRegistered());
        assertEquals("client", registrationInfo.getNodeId());
        try {
            restService.getPullData(registrationInfo.getNodeId(), "wrong password", false, false, true, null);
            fail("We should have received an exception");
        } catch (NotAllowedException ex) {
        }
        PullDataResults results = null;
        assertPullReturnsNoData(restService, registrationInfo);
        engine.getSqlTemplate().update("insert into a values(?, ?, ?)", 1, "this is a test", FormatUtils.parseDate("2013-06-08 00:00:00.000",
                FormatUtils.TIMESTAMP_PATTERNS));
        engine.route();
        results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), false, false, true, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(1, results.getNbrBatches());
        assertEquals(4, results.getBatches().get(0).getBatchId());
        log.info(results.getBatches().get(0).getSqlStatements().get(0));
        // pull a second time without acking. should get the same results
        results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), false, false, false, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(1, results.getNbrBatches());
        // test that when we don't request jdbc timestamp format sql statements come back in that format
        assertFalse(results.getBatches().get(0).getSqlStatements().get(0).contains("{ts '"));
        // make sure we have no delimited identifiers
        assertFalse(results.getBatches().get(0).getSqlStatements().get(0).contains("\""));
        engine.getSqlTemplate().update("update a set notes=? where id=?", "changed", 1);
        engine.getSqlTemplate().update("update a set notes=? where id=?", "changed again", 1);
        engine.route();
        results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), true, false, true, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(2, results.getNbrBatches());
        assertNotSame(results.getBatches().get(1).getBatchId(), results.getBatches().get(0).getBatchId());
        assertEquals(2, results.getBatches().get(1).getSqlStatements().size());
        // test that when we request jdbc timestamp format sql statements come back in that format
        String testSql = results.getBatches().get(1).getSqlStatements().get(0);
        assertTrue("The following sql was supposed to contain '{ts '" + testSql, testSql.contains("{ts '"));
        // make sure we have delimited identifiers
        assertTrue(results.getBatches().get(1).getSqlStatements().get(0).contains("\""));
        log.info(results.getBatches().get(1).getSqlStatements().get(0));
        log.info(results.getBatches().get(1).getSqlStatements().get(1));
        ackBatches(restService, registrationInfo, results, buildBatchResults(registrationInfo, results));
        engine.getSqlTemplate().update("insert into a values(?, ?, ?)", 2, "this is a test", FormatUtils.parseDate("2073-06-08 00:00:00.000",
                FormatUtils.TIMESTAMP_PATTERNS));
        engine.getSqlTemplate().update("insert into a values(?, ?, ?)", 3, "this is a test", FormatUtils.parseDate("2073-06-08 00:00:00.000",
                FormatUtils.TIMESTAMP_PATTERNS));
        engine.getSqlTemplate().update("update a set notes=? where id=?", "update to 2", 2);
        engine.getSqlTemplate().update("update a set notes=? where id=?", "update to 3", 3);
        engine.getSqlTemplate().update("update a set notes=? where id=?", "update 2 again", 2);
        engine.route();
        results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), false, true, true, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(1, results.getNbrBatches());
        List<String> sqls = results.getBatches().get(0).getSqlStatements();
        assertEquals(5, sqls.size());
        for (String sql : sqls) {
            log.info(sql);
            assertTrue(sql, sql.toLowerCase().startsWith("insert or replace"));
        }
        ackBatches(restService, registrationInfo, results, buildBatchResults(registrationInfo, results));
        Channel channel = engine.getConfigurationService().getChannel("default");
        channel.setBatchAlgorithm("nontransactional");
        channel.setMaxBatchSize(1);
        engine.getConfigurationService().saveChannel(channel, true);
        engine.getSqlTemplate().update("delete from a");
        engine.route();
        results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), false, false, true, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(3, results.getNbrBatches());
        List<Batch> batches = results.getBatches();
        for (Batch batch : batches) {
            assertEquals(1, batch.getSqlStatements().size());
            assertTrue(batch.getSqlStatements().get(0).toLowerCase().startsWith("delete from"));
        }
        ackBatches(restService, registrationInfo, results, buildBatchResults(registrationInfo, results));
    }

    protected BatchResults buildBatchResults(RegistrationInfo registrationInfo, PullDataResults results) {
        BatchResults batchResults = new BatchResults();
        batchResults.setNodeId(registrationInfo.getNodeId());
        for (Batch batch : results.getBatches()) {
            batchResults.getBatchResults().add(new BatchResult(batch.getBatchId(), true));
        }
        return batchResults;
    }

    protected void ackBatches(RestService restService, RegistrationInfo registrationInfo, PullDataResults results, BatchResults batchResults) {
        restService.putAcknowledgeBatch("server", registrationInfo.getNodePassword(), batchResults);
        assertPullReturnsNoData(restService, registrationInfo);
    }

    protected void assertPullReturnsNoData(RestService restService, RegistrationInfo registrationInfo) {
        PullDataResults results = restService.getPullData("server", registrationInfo.getNodeId(),
                registrationInfo.getNodePassword(), false, false, true, null);
        assertNotNull("Should have a non null results object", results);
        assertEquals(0, results.getNbrBatches());
    }
}
