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
package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.Assert;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class SqlScriptUnitTest {

    @Test
    public void testSimpleSqlScript() throws Exception {
        SingleConnectionDataSource ds = getDataSource();
        IDatabasePlatform platform = JdbcDatabasePlatformFactory.createNewPlatformInstance(ds, new SqlTemplateSettings());
        SqlScript script = new SqlScript(getClass().getResource("sqlscript-simple.sql"), platform.getSqlTemplate());
        script.execute();
        JdbcTemplate template = new JdbcTemplate(ds);
        Assert.assertEquals(2, template.queryForInt("select count(*) from test"));
        Assert.assertEquals(3, template.queryForObject("select test from test where test_id=2", String.class).split("\r\n|\r|\n").length);
        ds.destroy();
    }
    
    private SingleConnectionDataSource getDataSource() throws Exception {
        Class.forName("org.h2.Driver");
        Connection c = DriverManager.getConnection("jdbc:h2:mem:sqlscript");
        return new SingleConnectionDataSource(c, true);
    }
}