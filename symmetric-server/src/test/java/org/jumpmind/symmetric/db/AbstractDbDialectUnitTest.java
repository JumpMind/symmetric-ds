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
package org.jumpmind.symmetric.db;

import junit.framework.Assert;

import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class AbstractDbDialectUnitTest {

    @Test
    public void testOrderColumns() {
        AbstractDbDialect dbDialect = getAbstractDbDialect();
        Table table = new Table();
        Column col = new Column();
        col.setName("2");
        table.addColumn(col);
        col = new Column();
        col.setName("3");
        table.addColumn(col);
        col = new Column();
        col.setName("1");
        table.addColumn(col);
        col = new Column();
        col.setName("6");
        table.addColumn(col);
        col = new Column();
        col.setName("5");
        table.addColumn(col);
        col = new Column();
        col.setName("4");
        table.addColumn(col);
        String[] columnNames = { "1", "2", "3", "4", "5", "6" };
        Column[] ordered = dbDialect.orderColumns(columnNames, table);
        Assert.assertEquals(6, ordered.length);
        for (int i = 1; i <= 6; i++) {
            Assert.assertEquals(Integer.toString(i), ordered[i - 1].getName());
        }
    }

    @Test
    public void testOrderColumnsWithExtraColumnAtEnd() {
        AbstractDbDialect dbDialect = getAbstractDbDialect();
        Table table = new Table();
        Column col = new Column();
        col.setName("2");
        table.addColumn(col);
        col = new Column();
        col.setName("3");
        table.addColumn(col);
        col = new Column();
        col.setName("1");
        table.addColumn(col);
        col = new Column();
        col.setName("6");
        table.addColumn(col);
        col = new Column();
        col.setName("5");
        table.addColumn(col);
        col = new Column();
        col.setName("4");
        table.addColumn(col);
        col = new Column();
        col.setName("7");
        table.addColumn(col);
        String[] columnNames = { "1", "2", "3", "4", "5", "6" };
        Column[] ordered = dbDialect.orderColumns(columnNames, table);
        Assert.assertEquals(6, ordered.length);
        for (int i = 1; i <= 6; i++) {
            Assert.assertEquals(Integer.toString(i), ordered[i - 1].getName());
        }
    }

    @Test
    public void testOrderColumnsWithExtraColumnAtInMiddle() {
        AbstractDbDialect dbDialect = getAbstractDbDialect();
        Table table = new Table();
        Column col = new Column();
        col.setName("2");
        table.addColumn(col);
        col = new Column();
        col.setName("3");
        table.addColumn(col);
        col = new Column();
        col.setName("7");
        table.addColumn(col);
        col = new Column();
        col.setName("8");
        table.addColumn(col);
        col = new Column();
        col.setName("1");
        table.addColumn(col);
        col = new Column();
        col.setName("6");
        table.addColumn(col);
        col = new Column();
        col.setName("10");
        table.addColumn(col);
        col = new Column();
        col.setName("5");
        table.addColumn(col);
        col = new Column();
        col.setName("4");
        table.addColumn(col);

        String[] columnNames = { "1", "2", "3", "4", "5", "6" };
        Column[] ordered = dbDialect.orderColumns(columnNames, table);
        Assert.assertEquals(6, ordered.length);
        for (int i = 1; i <= 6; i++) {
            Assert.assertEquals(Integer.toString(i), ordered[i - 1].getName());
        }
    }

    @Test
    public void testOrderColumnsWithMissingColumn() {
        AbstractDbDialect dbDialect = getAbstractDbDialect();
        Table table = new Table();
        Column col = new Column();
        col.setName("2");
        table.addColumn(col);
        col = new Column();
        col.setName("1");
        table.addColumn(col);
        col = new Column();
        col.setName("6");
        table.addColumn(col);
        col = new Column();
        col.setName("5");
        table.addColumn(col);
        col = new Column();
        col.setName("4");
        table.addColumn(col);

        String[] columnNames = { "1", "2", "3", "4", "5", "6" };
        Column[] ordered = dbDialect.orderColumns(columnNames, table);
        Assert.assertEquals(6, ordered.length);
        for (int i = 1; i <= 6; i++) {
            if (i != 3) {
                Assert.assertEquals(Integer.toString(i), ordered[i - 1].getName());
            } else {
                Assert.assertNull(ordered[i - 1]);
            }
        }
    }

    protected AbstractDbDialect getAbstractDbDialect() {
        return new AbstractDbDialect() {

            public void purge() {
            }

            public boolean isNonBlankCharColumnSpacePadded() {
                return false;
            }

            public boolean isEmptyStringNulled() {
                return false;
            }

            public boolean isCharColumnSpaceTrimmed() {
                return false;
            }

            public String getSyncTriggersExpression() {
                return null;
            }

            public String getDefaultCatalog() {
                return null;
            }

            public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
            }

            public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
            }

            @Override
            protected void initTablesAndFunctionsForSpecificDialect() {
            }

            @Override
            protected boolean doesTriggerExistOnPlatform(String catalogName, String schema,
                    String tableName, String triggerName) {
                return false;
            }
        };
    }

}