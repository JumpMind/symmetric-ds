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

import static org.junit.Assert.*;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.web.rest.RestService;

public class NonDmlEventsTest extends AbstractTest {
    // test sendSchema
    // test sendScript
    // test send reload to multiple nodes
    Table testTable;

    @Override
    protected Table[] getTables(String name) {
        testTable = new Table("CamelCase");
        testTable.addColumn(new Column("Id", true, Types.BIGINT, -1, -1));
        testTable.addColumn(new Column("Notes", false, Types.VARCHAR, 255, 0));
        Table a = new Table("A");
        a.addColumn(new Column("ID", true, Types.BIGINT, -1, -1));
        Table b = new Table("B");
        b.addColumn(new Column("ID", true, Types.BIGINT, -1, -1));
        Table nodeSpecific = new Table("NODE_SPECIFIC");
        nodeSpecific.addColumn(new Column("NODE_ID", true, Types.BIGINT, -1, -1));
        return new Table[] { testTable, a, b, nodeSpecific };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        loadConfigAndRegisterNode("client", "root");
        // pull to clear out any heartbeat events
        pull("client");
        List<TriggerHistory> histories = rootServer.getTriggerRouterService().findTriggerHistories(
                null, null, testTable.getName());
        assertNotNull(histories);
        assertEquals(1, histories.size());
        String serverQuote = rootServer.getDatabasePlatform().getDatabaseInfo().getDelimiterToken();
        String clientQuote = clientServer.getDatabasePlatform().getDatabaseInfo()
                .getDelimiterToken();
        for (int i = 0; i < 100; i++) {
            rootServer.getSqlTemplate().update(
                    String.format("insert into %sCamelCase%s values (?,?)", serverQuote,
                            serverQuote), i, "this is a test");
        }
        String serverCountSql = String.format("select count(*) from %sCamelCase%s", serverQuote,
                serverQuote);
        String clientCountSql = String.format("select count(*) from %sCamelCase%s", clientQuote,
                clientQuote);
        assertEquals(100,
                rootServer.getDatabasePlatform().getSqlTemplate().queryForInt(serverCountSql));
        assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));
        // we installed a dead trigger, so no data should have been captured
        assertFalse(pull("client"));
        rootServer.getDataService().reloadTable("client", null, null, testTable.getName());
        assertTrue(pull("client"));
        assertEquals(100,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));
        rootServer.getDataService().sendSQL(
                "client",
                null,
                null,
                testTable.getName(),
                String.format("insert into %sCamelCase%s values (101,'direct insert')",
                        clientQuote, clientQuote));
        rootServer.getDataService().sendSQL(
                "client",
                null,
                null,
                testTable.getName(),
                String.format("insert into %sCamelCase%s values (102,'direct insert')",
                        clientQuote, clientQuote));
        assertTrue(pull("client"));
        assertEquals(102,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));
        rootServer.getDataService().sendSQL("client", null, null, testTable.getName(),
                String.format("delete from %sCamelCase%s", clientQuote, clientQuote));
        assertTrue(pull("client"));
        assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));
        rootServer.getDataService().reloadTable("client", null, null, testTable.getName(),
                String.format("%sId%s < 50", serverQuote, serverQuote));
        assertTrue(pull("client"));
        assertEquals(50,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));
        Table serverTable = rootServer.getDatabasePlatform().readTableFromDatabase(null, null, "A");
        Table clientTable = clientServer.getDatabasePlatform().readTableFromDatabase(null, null, "A");
        // test a wildcard table
        for (int i = 0; i < 10; i++) {
            rootServer.getSqlTemplate().update(String.format("insert into %s values (?)", serverTable.getName()), i);
        }
        assertFalse(pull("client"));
        String msg = rootServer.getDataService()
                .reloadTable("client", null, null, "A");
        assertTrue(
                "Should have pulled data for the reload event for table A.  The reload table method returned the following text: "
                        + msg, pull("client"));
        assertEquals(
                10,
                clientServer.getDatabasePlatform().getSqlTemplate()
                        .queryForInt(String.format("select count(*) from %s", clientTable.getName())));
        testRoutingOfReloadEvents(rootServer, clientServer);
    }

    protected void testRoutingOfReloadEvents(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        rootServer.getParameterService().saveParameter(ParameterConstants.REST_API_ENABLED, true, "unit_test");
        rootServer.getRegistrationService().openRegistration(clientServer.getParameterService().getNodeGroupId(), "2");
        rootServer.getRegistrationService().openRegistration(clientServer.getParameterService().getNodeGroupId(), "3");
        RestService restService = getRegServer().getRestService();
        /* register a few more nodes to make sure that when we insert reload events they are only routed to the node we want */
        restService.postRegisterNode("2", clientServer.getParameterService().getNodeGroupId(), DatabaseNamesConstants.H2, "1.2", "host2");
        restService.postRegisterNode("3", clientServer.getParameterService().getNodeGroupId(), DatabaseNamesConstants.H2, "1.2", "host2");
        rootServer.route();
        assertEquals(0, rootServer.getOutgoingBatchService().countOutgoingBatchesUnsent(Constants.CHANNEL_RELOAD));
        Table serverTable = rootServer.getDatabasePlatform().readTableFromDatabase(null, null, "NODE_SPECIFIC");
        assertNotNull(serverTable);
        assertTrue(rootServer.getDataService().reloadTable(clientServer.getNodeService().findIdentityNodeId(), null, null, serverTable.getName()).startsWith(
                "Successfully created"));
        rootServer.route();
        assertEquals(0, rootServer.getOutgoingBatchService().getOutgoingBatches("2", true).getBatchesForChannel(Constants.CHANNEL_RELOAD).size());
        assertEquals(0, rootServer.getOutgoingBatchService().getOutgoingBatches("3", true).getBatchesForChannel(Constants.CHANNEL_RELOAD).size());
        OutgoingBatches batches = rootServer.getOutgoingBatchService().getOutgoingBatches(clientServer.getNodeService().findIdentityNodeId(), true);
        assertEquals(1, batches.getBatchesForChannel(Constants.CHANNEL_RELOAD).size());
    }
}
