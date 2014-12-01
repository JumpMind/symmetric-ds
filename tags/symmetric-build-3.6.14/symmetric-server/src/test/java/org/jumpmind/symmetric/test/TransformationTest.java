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

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.symmetric.ISymmetricEngine;

public class TransformationTest extends AbstractTest {

    Table srcTableA;

    Table tgtTableA;

    @Override
    protected Table[] getTables(String name) {
        srcTableA = new Table("TRANSFORM_TABLE_A_SRC");
        srcTableA.addColumn(new Column("SRC_ID", true, Types.VARCHAR, 255, 0));
        srcTableA.addColumn(new Column("COL1", false, Types.INTEGER, -1, -1));

        tgtTableA = new Table("TRANSFORM_TABLE_A_TGT");
        tgtTableA.addColumn(new Column("TGT_ID", true, Types.VARCHAR, 255, 0));
        tgtTableA.addColumn(new Column("COL1", false, Types.INTEGER, -1, -1));

        return new Table[] { srcTableA, tgtTableA };
    }

    @Override
    protected String[] getGroupNames() {
        return new String[] { "root", "client" };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        loadConfigAndRegisterNode("client", "root");
        testDeletesWithTransformedIdWork(rootServer, clientServer);
    }

    protected void testDeletesWithTransformedIdWork(ISymmetricEngine rootServer,
            ISymmetricEngine clientServer) throws Exception {
        
        String rootTableName = rootServer.getDatabasePlatform().getTableFromCache("TRANSFORM_TABLE_A_SRC", false).getName();
        String clientTableName = clientServer.getDatabasePlatform().getTableFromCache("TRANSFORM_TABLE_A_TGT", false).getName();
        
        ISqlTemplate rootTemplate = rootServer.getDatabasePlatform().getSqlTemplate();
        ISqlTemplate clientTemplate = clientServer.getDatabasePlatform().getSqlTemplate();
        
        rootTemplate.update(String.format("insert into %s values(?,?)", rootTableName), "1", 1);
        assertEquals(0, clientTemplate.queryForInt(String.format("select count(*) from %s",clientTableName)));
        pull("client");
        assertEquals(1, clientTemplate.queryForInt(String.format("select count(*) from %s",clientTableName)));
        rootTemplate.update(String.format("delete from %s", rootTableName));
        assertEquals(1, clientTemplate.queryForInt(String.format("select count(*) from %s",clientTableName)));
        pull("client");
        assertEquals(0, clientTemplate.queryForInt(String.format("select count(*) from %s",clientTableName)));
    }
}
