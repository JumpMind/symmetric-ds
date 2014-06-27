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
package org.jumpmind.db.io;

import junit.framework.Assert;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.junit.Test;

public class DatabaseXmlUtilTest {
    
    @Test
    public void testReadXml() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabaseIO.xml"));
        Assert.assertNotNull(database);
        Assert.assertEquals(1, database.getTableCount());
        Assert.assertEquals("test", database.getName());
        
        Table table = database.getTable(0);
        Assert.assertEquals("test_simple_table", table.getName());
        Assert.assertEquals(8, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("id", table.getPrimaryKeyColumnNames()[0]);
               
    }

}
