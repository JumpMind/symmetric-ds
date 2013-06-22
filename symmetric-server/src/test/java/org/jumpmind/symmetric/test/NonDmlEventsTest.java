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
    
    Table camelCase;

    @Override
    protected Table[] getTables(String name) {
        camelCase = new Table("CamelCase");
        camelCase.addColumn(new Column("Id", true, Types.BIGINT, -1, -1));
        camelCase.addColumn(new Column("Notes", false, Types.VARCHAR, 255, 0));
        return new Table[] { camelCase };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer)
            throws Exception {
        loadConfigAndRegisterNode("client", "root");
        
        List<TriggerHistory> histories = rootServer.getTriggerRouterService().findTriggerHistories(null, null, camelCase.getName());
        Assert.assertNotNull(histories);
        Assert.assertEquals(1, histories.size());
    }

}
