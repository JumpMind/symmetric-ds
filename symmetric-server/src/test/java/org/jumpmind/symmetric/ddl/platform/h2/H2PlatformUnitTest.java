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
package org.jumpmind.symmetric.ddl.platform.h2;

import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.Assert;

import org.h2.Driver;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.PlatformFactory;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class H2PlatformUnitTest {

    private static final String H2 = "H2";


    @Test
    public void testColumnSizeChangeToVarchar() throws Exception {
        Platform pf = getPlatform();
        Database db = new Database();
        Table table = new Table();
        table.setName("TEST_TABLE_VARCHAR");
        Column column1 = new Column();
        column1.setName("COLUMN_1");
        column1.setType("VARCHAR");
        column1.setSize("20");
        table.addColumn(column1);
        db.addTable(table);
        pf.createTables(db, false, false);
        
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(pf.getDataSource());
        Assert.assertEquals(0, jdbcTemplate.queryForInt("select count(*) from TEST_TABLE_VARCHAR"));
        Assert.assertEquals(20, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_VARCHAR' and COLUMN_NAME='COLUMN_1'"));
        
        Database readDb = pf.readModelFromDatabase(null);
        Table[] tables = readDb.getTables();
        for (Table t : tables) {
            if (t.getName().equals("TEST_TABLE_VARCHAR")) {
                Assert.assertEquals(20, t.getColumn(0).getSizeAsInt());
            }
        }
        
        column1.setSize("50");
        pf.alterTables(null, null, null, db, false);
        Assert.assertEquals(50, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_VARCHAR' and COLUMN_NAME='COLUMN_1'"));
    }
    
    @Test
    public void testColumnSizeChangeToNumeric() throws Exception {
        Platform pf = getPlatform();
        Database db = new Database();
        Table table = new Table();
        table.setName("TEST_TABLE_NUMERIC");
        Column column1 = new Column();
        column1.setName("COLUMN_1");
        column1.setType("NUMERIC");
        column1.setSize("15");        
        table.addColumn(column1);
        db.addTable(table);
        pf.createTables(db, false, false);
        
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(pf.getDataSource());
        Assert.assertEquals(0, jdbcTemplate.queryForInt("select count(*) from TEST_TABLE_NUMERIC"));
        Assert.assertEquals(15, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_NUMERIC' and COLUMN_NAME='COLUMN_1'"));
        
        column1.setSize("200");
        pf.alterTables(null, null, null, db, false);
        Assert.assertEquals(200, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_NUMERIC' and COLUMN_NAME='COLUMN_1'"));
    }

    @Test
    public void testChangeDefaultValue() throws Exception {
        Platform pf = getPlatform();
        Database db = new Database();
        Table table = new Table();
        table.setName("TEST_TABLE_DEFAULTS");
        Column column1 = new Column();
        column1.setName("COLUMN_1");
        column1.setType("NUMERIC");
        column1.setSize("15");        
        column1.setDefaultValue("1");
        Column column2 = new Column();
        column2.setName("COLUMN_2");
        column2.setType("VARCHAR");
        column2.setSize("50");
        column2.setDefaultValue("test");
        table.addColumn(column1);
        table.addColumn(column2);
        db.addTable(table);
        pf.createTables(db, false, false);
        
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(pf.getDataSource());
        Assert.assertEquals(0, jdbcTemplate.queryForInt("select count(*) from TEST_TABLE_DEFAULTS"));
        Assert.assertEquals("1", jdbcTemplate.queryForObject("select COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_DEFAULTS' and COLUMN_NAME='COLUMN_1'", String.class));
        Assert.assertEquals("'test'", jdbcTemplate.queryForObject("select COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_DEFAULTS' and COLUMN_NAME='COLUMN_2'", String.class));
        
        column1.setDefaultValue("2");
        column2.setDefaultValue(null);
        pf.alterTables(null, null, null, db, false);
        Assert.assertEquals("2", jdbcTemplate.queryForObject("select COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_DEFAULTS' and COLUMN_NAME='COLUMN_1'", String.class));
        Assert.assertEquals(null, jdbcTemplate.queryForObject("select COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE_DEFAULTS' and COLUMN_NAME='COLUMN_2'", String.class));
    }

    protected Platform getPlatform() throws Exception {
        PlatformFactory.registerPlatform(H2, H2Platform.class);
        Platform pf = PlatformFactory.createNewPlatformInstance(H2);
        Class.forName(Driver.class.getName());
        Connection c = DriverManager.getConnection("jdbc:h2:mem:test", "test", "test");
        pf.setDataSource(new SingleConnectionDataSource(c, true));
        return pf;
    }
}