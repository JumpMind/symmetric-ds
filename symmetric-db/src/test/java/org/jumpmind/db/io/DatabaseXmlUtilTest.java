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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DatabaseXmlUtilTest {
    @Test
    public void testReadXml_EssentialFields() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabaseIO.xml"));
        assertNotNull(database);
        assertEquals(2, database.getTableCount());
        assertEquals("test", database.getName());
        Table table = database.getTable(0);
        assertEquals("test_simple_table", table.getName());
        assertEquals(8, table.getColumnCount());
        assertEquals(1, table.getPrimaryKeyColumnCount());
        assertEquals("id", table.getPrimaryKeyColumnNames()[0]);
        Table tableWithAmp = database.getTable(1);
        assertEquals("testColumnWith&", tableWithAmp.getName());
        assertEquals("&Amp", tableWithAmp.getColumn(0).getName());
    }

    @Test
    public void testWriteXml_ForTableWithAmpersand() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabaseIO.xml"));
        Table tableWithAmp = database.getTable(1);
        StringWriter writer = new StringWriter();
        DatabaseXmlUtil.write(tableWithAmp, writer);
        String xml = writer.getBuffer().toString();
        assertTrue(xml.contains("\"testColumnWith&amp;\""));
        assertTrue(xml.contains("\"&amp;Amp\""));
    }

    @Test
    public void testWriteXml_ForTableWithoutLogging() {
        Database database = DatabaseXmlUtil.read(getClass().getResourceAsStream("/testDatabaseIO.xml"));
        Table tableWithoutLogging = database.getTable(1);
        tableWithoutLogging.setLogging(false);
        String xml = DatabaseXmlUtil.toXml(database);
        // System.out.println("testWriteXml_ForTableWithoutLogging xml=" + xml);
        assertTrue(xml.contains(" logging=\"false\""));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    public void testReadXml_ForTableWithoutLogging(int tableLoggingMode) {
        String tableLoggingStr = (tableLoggingMode == 1) ? "true" : "false";
        String xml = "<database name=\"test\">"
                + "<table name=\"testColumnWith&amp;\" logging=\"" + tableLoggingStr + "\">\n"
                + "        <column name=\"&amp;Amp\" type=\"VARCHAR\" size=\"50\"/>\n"
                + "    </table></database>\n";
        StringReader reader = new StringReader(xml);
        Database database = DatabaseXmlUtil.read(reader, false);
        Table tableWithoutLogging = database.getTable(0);
        // System.out.println(String.format("testReadXml_ForTableWithoutLogging Logging=%b", tableWithoutLogging.getLogging()));
        assertTrue((tableLoggingMode == 1) == tableWithoutLogging.getLogging());
    }
}
