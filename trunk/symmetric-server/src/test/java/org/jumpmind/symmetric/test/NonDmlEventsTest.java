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

    // test 2 reload events in same batch
    // test wildcard table reloads
    // test sendSql
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
        a.addColumn(new Column("Id", true, Types.BIGINT, -1, -1));

        Table b = new Table("B");
        b.addColumn(new Column("Id", true, Types.BIGINT, -1, -1));

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

        String quote = rootServer.getDatabasePlatform().getDatabaseInfo().getDelimiterToken();
        for (int i = 0; i < 100; i++) {
            rootServer.getSqlTemplate().update(
                    String.format("insert into %sCamelCase%s values (?,?)", quote, quote), i,
                    "this is a test");
        }

        String countSql = String.format("select count(*) from %sCamelCase%s", quote, quote);
        Assert.assertEquals(100,
                rootServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));
        Assert.assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));

        // we installed a dead trigger, so no data should have been captured
        Assert.assertFalse(pull("client"));

        rootServer.getDataService().reloadTable("client", null, null, testTable.getName());

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(100,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));

        String clientQuote = clientServer.getDatabasePlatform().getDatabaseInfo()
                .getDelimiterToken();

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
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));

        rootServer.getDataService().sendSQL("client", null, null, testTable.getName(),
                String.format("delete from %sCamelCase%s", clientQuote, clientQuote));

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(0,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));

        rootServer.getDataService().reloadTable("client", null, null, testTable.getName(),
                String.format("%sId%s < 50", quote, quote));

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(50,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt(countSql));
        
        // test a wildcard table
        for (int i = 0; i < 10; i++) {
            rootServer.getSqlTemplate().update("insert into a values (?)", i);
        }
        
        Assert.assertFalse(pull("client"));
        
        rootServer.getDataService().reloadTable("client", null, null, "A");

        Assert.assertTrue(pull("client"));

        Assert.assertEquals(10,
                clientServer.getDatabasePlatform().getSqlTemplate().queryForInt("select count(*) from A"));

    }

}
