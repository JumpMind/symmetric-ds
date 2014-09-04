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
package org.jumpmind.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

import java.io.StringReader;

import org.apache.log4j.Level;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractDdlTypesTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private static IDatabasePlatform platform;

    protected Level originalLevel;

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
    }

    protected abstract String getName();

    protected abstract String[] getDdlTypes();

    @Before
    public void checkDatabaseType() {
        assumeTrue(platform.getName().equals(getName()));
    }

    @Test
    public void testPlatformSpecificDdl() throws Exception {

        dropTable();

        createTable();

        Table fromDb1 = platform.readTableFromDatabase(null, null, tableName());
        assertNotNull(fromDb1);

        dropTable();

        Column[] columns1 = fromDb1.getColumns();
        for (Column column : columns1) {
            assertNotNull(column.findPlatformColumn(getName()));
        }

        String xml = DatabaseXmlUtil.toXml(fromDb1);
        
        log.info("XML generated for table:\n"+xml);

        StringReader reader = new StringReader(xml);
        Table fromXml = DatabaseXmlUtil.read(reader, false).getTable(0);
        for (Column column : fromXml.getColumns()) {
            assertNotNull("Expected " + getName() + " platform specific column information for "
                    + column.getName(), column.findPlatformColumn(getName()));
        }

        assertNotNull(fromXml);

        platform.alterTables(false, fromXml);

        Table fromDb2 = platform.readTableFromDatabase(null, null, tableName());
        assertNotNull("Could not find " + tableName() + " in the database", fromDb2);

        for (Column column1 : columns1) {
            PlatformColumn pColumn1 = column1.findPlatformColumn(getName());
            Column column2 = fromDb2.findColumn(column1.getName());
            assertNotNull(column2);
            PlatformColumn pColumn2 = column2.findPlatformColumn(getName());
            assertNotNull(pColumn2);
            assertEquals("Column types not equals for column " + column1.getName(), pColumn1.getType(), pColumn2.getType());
            assertEquals(pColumn1.getSize(), pColumn2.getSize());
            assertEquals(pColumn1.getDecimalDigits(), pColumn2.getDecimalDigits());
        }

    }

    protected void createTable() {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        sqlTemplate.update(buildDdl());
    }

    protected void dropTable() {
        Table table = platform.readTableFromDatabase(null, null, tableName());
        if (table != null) {
            platform.dropTables(true, table);
        }
    }

    protected String tableName() {
        return platform.alterCaseToMatchDatabaseDefaultCase("test_types");
    }

    protected String buildDdl() {
        StringBuilder ddl = new StringBuilder();
        String[] colTypes = getDdlTypes();
        ddl.append("CREATE TABLE ").append(tableName()).append(" (");
        for (int i = 0; i < colTypes.length; i++) {
            ddl.append(platform.alterCaseToMatchDatabaseDefaultCase("col")).append(i).append(" ")
                    .append(colTypes[i]).append(",");
        }
        ddl.replace(ddl.length() - 1, ddl.length(), ")");
        return ddl.toString();
    }
}
