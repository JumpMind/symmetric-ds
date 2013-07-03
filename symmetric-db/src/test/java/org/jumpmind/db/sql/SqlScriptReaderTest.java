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
 * under the License. 
 */
package org.jumpmind.db.sql;

import java.io.InputStreamReader;

import junit.framework.Assert;

import org.junit.Test;

public class SqlScriptReaderTest {

    @Test
    public void testReadScript() throws Exception {
        SqlScriptReader reader = new SqlScriptReader(new InputStreamReader(getClass().getResourceAsStream("/test-script-1.sql")));         
        Assert.assertEquals("select * from \n  TEST where\nid = 'someid'", reader.readSqlStatement());
        Assert.assertEquals("select * from test", reader.readSqlStatement());
        Assert.assertEquals("insert into test (one, two, three) values('1','1','2')", reader.readSqlStatement());
        for (int i = 0; i < 4; i++) {            
           Assert.assertEquals("delete from test where one='1'", reader.readSqlStatement());
        }
        Assert.assertEquals("update sym_node set sync_url='http://localhost:8080/test' where node_id='test'", reader.readSqlStatement());
        Assert.assertEquals("update something set oops=';' where whoops='test'", reader.readSqlStatement());
        Assert.assertEquals("update test set one = '''', two='\\\\##--''' where one is null", reader.readSqlStatement());
        Assert.assertEquals("update test\n  set one = '1', two = '2'\nwhere one = 'one'", reader.readSqlStatement());
        Assert.assertNull(reader.readSqlStatement());
        reader.close();
    }
}
