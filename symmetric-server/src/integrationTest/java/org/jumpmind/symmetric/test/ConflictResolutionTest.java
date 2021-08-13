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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Types;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingError;

public class ConflictResolutionTest extends AbstractTest {
    Table testTableA;

    @Override
    protected Table[] getTables(String name) {
        testTableA = new Table("conflict_table_a");
        testTableA.addColumn(new Column("id", true, Types.VARCHAR, 255, 0));
        testTableA.addColumn(new Column("string_a", false, Types.VARCHAR, 255, 0));
        testTableA.addColumn(new Column("tm", false, Types.TIMESTAMP, -1, -1));
        return new Table[] { testTableA };
    }

    @Override
    protected String[] getGroupNames() {
        return new String[] { "server", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer) throws Exception {
        loadConfigAndRegisterNode("client", "server");
        push("client");
        testBasicSync(rootServer, clientServer);
        testExistingRowInServerInsertOnClient(rootServer, clientServer);
        testUpdateRowOnServerThenUpdateRowOnClient(rootServer, clientServer);
        /*
         * assertFalse(pullFiles());
         * 
         * testInitialLoadFromServerToClient(rootServer, clientServer);
         * 
         * testPullAllFromServerToClient(rootServer, clientServer);
         * 
         * testPingback();
         * 
         * testChooseTargetDirectory(rootServer, clientServer);
         * 
         * testChangeFileNameAndCreateTargetDir(rootServer, clientServer);
         */
    }

    protected void testBasicSync(ISymmetricEngine rootServer, ISymmetricEngine clientServer) throws Exception {
        clientServer.getSqlTemplate().update(
                clientServer.getDatabasePlatform().scrubSql(String.format("insert into %s values (?,?,current_timestamp)", testTableA.getName())), "1", "c1");
        push("client");
        assertEquals(1, rootServer.getSqlTemplate().queryForInt("select count(*) from conflict_table_a"));
        assertEquals("c1", rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a"));
    }

    protected void testExistingRowInServerInsertOnClient(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        rootServer.getSqlTemplate().update(
                rootServer.getDatabasePlatform().scrubSql(String.format("insert into %s values (?,?,current_timestamp)", testTableA.getName())), "2", "s1");
        assertEquals(2, rootServer.getSqlTemplate().queryForInt("select count(*) from conflict_table_a"));
        assertEquals("s1",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
        clientServer.getSqlTemplate().update(
                clientServer.getDatabasePlatform().scrubSql(String.format("insert into %s values (?,?,current_timestamp)", testTableA.getName())), "2", "c1");
        assertTrue(push("client"));
        assertEquals("s1",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
        IncomingError error = getOnlyIncomingError(rootServer, "client");
        String rowData = error.getRowData();
        assertTrue(rowData.contains("\"c1\""));
        String resolveData = rowData.replace("\"c1\"", "\"s2\"");
        rootServer.getSqlTemplate().update("update sym_incoming_error set resolve_data=? where batch_id=?",
                resolveData, error.getBatchId());
        assertTrue(push("client"));
        assertEquals("s2",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
    }

    public IncomingError getOnlyIncomingError(ISymmetricEngine server, String nodeId) {
        // assertEquals(1,
        // server.getIncomingBatchService().countIncomingBatchesInError());
        List<IncomingBatch> errorBatches = server.getIncomingBatchService().findIncomingBatchErrors(12);
        if (errorBatches == null || errorBatches.size() == 0) {
            return null;
        }
        long errorBatch = errorBatches.get(0).getBatchId();
        List<IncomingError> errors = server.getDataLoaderService().getIncomingErrors(errorBatch, nodeId);
        assertEquals(1, errors.size());
        return errors.get(0);
    }
    // Tests simultaneous updates to an existing row with resolution based on resolve data and then same scenario again
    // with resolution based on resolve_ignore.

    protected void testUpdateRowOnServerThenUpdateRowOnClient(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        int count = rootServer.getSqlTemplate().update(
                String.format("update %s set string_a=? where id=?", testTableA.getName()), "notS3", "2");
        assertEquals(1, count);
        count = clientServer.getSqlTemplate().update(
                String.format("update %s set string_a=? where id=?", testTableA.getName()), "S3", "2");
        assertEquals(1, count);
        assertTrue(push("client"));
        assertEquals("notS3",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
        IncomingError error = getOnlyIncomingError(rootServer, "client");
        String rowData = error.getRowData();
        assertTrue(rowData.contains("\"S3\""));
        String resolveData = rowData.replace("\"S3\"", "\"S4\"");
        rootServer.getSqlTemplate().update("update sym_incoming_error set resolve_data=? where batch_id=?",
                resolveData, error.getBatchId());
        assertTrue(push("client"));
        error = getOnlyIncomingError(rootServer, "client");
        assertEquals(null, error);
        assertEquals("S4",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
        // Same test, but we're ignore row this time
        count = rootServer.getSqlTemplate().update(
                String.format("update %s set string_a=? where id=?", testTableA.getName()), "notS5", "2");
        assertEquals(1, count);
        count = clientServer.getSqlTemplate().update(
                String.format("update %s set string_a=? where id=?", testTableA.getName()), "S5", "2");
        assertEquals(1, count);
        assertTrue(push("client"));
        assertEquals("notS5",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
        error = getOnlyIncomingError(rootServer, "client");
        rowData = error.getRowData();
        rootServer.getSqlTemplate().update("update sym_incoming_error set resolve_ignore=? where batch_id=?",
                1, error.getBatchId());
        assertTrue(push("client"));
        error = getOnlyIncomingError(rootServer, "client");
        assertEquals(null, error);
        assertEquals("notS5",
                rootServer.getSqlTemplate().queryForString("select string_a from conflict_table_a where id='2'"));
    }
}
