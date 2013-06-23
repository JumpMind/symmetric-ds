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

import junit.framework.Assert;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.TriggerHistory;

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

        return new Table[] { testTable, a, b };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        loadConfigAndRegisterNode("client", "root");

        // pull to clear out any heartbeat events
        pull("client");

        List<TriggerHistory> histories = rootServer.getTriggerRouterService().findTriggerHistories(
                null, null, testTable.getName());
        Assert.assertNotNull(histories);
        Assert.assertEquals(1, histories.size());

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
        Assert.assertEquals(100,
                rootServer.getDatabasePlatform().getSqlTemplate().queryForInt(serverCountSql));
        Assert.assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));

        // we installed a dead trigger, so no data should have been captured
        Assert.assertFalse(pull("client"));

        rootServer.getDataService().reloadTable("client", null, null, testTable.getName());

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(100,
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

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(102,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));

        rootServer.getDataService().sendSQL("client", null, null, testTable.getName(),
                String.format("delete from %sCamelCase%s", clientQuote, clientQuote));

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));

        rootServer.getDataService().reloadTable("client", null, null, testTable.getName(),
                String.format("%sId%s < 50", serverQuote, serverQuote));

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(50,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(clientCountSql));

        // test a wildcard table
        for (int i = 0; i < 10; i++) {
            rootServer.getSqlTemplate().update(
                    "insert into A values (?)", i);
        }

        Assert.assertFalse(pull("client"));

        rootServer.getDataService().reloadTable("client", null, null, "A");

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(
                10,
                clientServer
                        .getDatabasePlatform()
                        .getSqlTemplate()
                        .queryForInt(
                                "select count(*) from A"));

    }
}
