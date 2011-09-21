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

package org.jumpmind.symmetric.load;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class AdditiveDataLoaderFilterTest extends AbstractDatabaseTest {

    private static final String TABLE_TEST_1 = "test_add_dl_table_1";
    private static final String TABLE_TEST_2 = "test_add_dl_table_2";

    private DataLoaderContext ctx1;
    private AdditiveDataLoaderFilter filter;
    private AdditiveDataLoaderFilter filter2;

    private DataLoaderContext ctx2;

    public AdditiveDataLoaderFilterTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {

        // Context 1 is for a filtered table; context 2, non-filtered.

        ctx1 = new DataLoaderContext(this.getNodeService(), this.getJdbcTemplate());
        ctx1.setSourceNodeId("54321");
        ctx1.setTableName(TABLE_TEST_1);
        ctx1.chooseTableTemplate();
        ctx1.setTableTemplate(new TableTemplate(getJdbcTemplate(), getDbDialect(), TABLE_TEST_1, null, false, null,
                null));
        ctx1.setColumnNames(new String[] { "pk1", "pk2", "add1", "add2", "add3", "ovr1", "ovr2", "ovr3", "nada1" });
        ctx1.setKeyNames(new String[] { "pk1", "pk2" });

        ctx2 = new DataLoaderContext(this.getNodeService(), this.getJdbcTemplate());
        ctx2.setSourceNodeId("54321");
        ctx2.setTableName(TABLE_TEST_2);
        ctx2.setTableTemplate(new TableTemplate(getJdbcTemplate(), getDbDialect(), TABLE_TEST_2, null, false, null,
                null));
        ctx2.setKeyNames(new String[] { "pk1" });
        ctx2.setColumnNames(new String[] { "pk1", "add1" });

        filter = new AdditiveDataLoaderFilter();
        filter.setTableName(TABLE_TEST_1);
        filter.setJdbcTemplate(getJdbcTemplate());
        filter.setAdditiveColumnNames(new String[] { "add1", "add2", "add3" });
        filter.setOverrideColumnNames(new String[] { "ovr1", "ovr2", "ovr3" });

        filter2 = new AdditiveDataLoaderFilter();
        filter2.setTableName(TABLE_TEST_1);
        filter2.setJdbcTemplate(getJdbcTemplate());
    }

    @Test
    public void testNonFilteredTable() {
        ctx2.setOldData(new String[] { "k1", "0" });
        Assert.assertTrue(filter.filterInsert(ctx2, new String[] { "k1", "1" }));
        Assert.assertTrue(filter.filterUpdate(ctx2, new String[] { "k1", "1" }, new String[] { "k1" }));
        Assert.assertTrue(filter.filterDelete(ctx2, new String[] { "k1" }));
    }

    @Test
    public void testInsertNonExistent() {
        ctx1.setOldData(new String[] { "k1", "k2", "0", "0.0", "0", "0.0", "0", "0.0", "5" });
        Assert.assertTrue(filter.filterInsert(ctx1,
                new String[] { "k1", "k2", "1", "0.0", "2", "3.5", "4", "0.0", "5" }));
    }

    @Test
    public void testDelete() {
        ctx1.setOldData(new String[] { "k1", "k2", "0", "0.0", "0", "0.0", "0", "0.0", "5" });
        try {
            filter.filterDelete(ctx1, new String[] { "k1", "k2" });
            Assert.fail("Delete should have thrown exception.");
        } catch (Exception e) {
            // expected.
        }
    }

    @Test
    public void testUpdateNonExistent() {
        ctx1.setOldData(new String[] { "k1", "k2", "0", "0.0", "0", "0.0", "0", "0.0", "5" });
        boolean result = filter.filterUpdate(ctx1,
                new String[] { "k1", "k2", "1.0", "0.0", "2", "3.5", "4", "0.0", "5" }, new String[] { "k1", "k2" });

        Assert.assertTrue(result);
    }

    @Test
    public void testUpdateNoSet() {
        ctx1.setOldData(new String[] {});
        boolean result = filter2.filterUpdate(ctx1, new String[] {}, new String[] {});

        Assert.assertTrue(result);
    }

    @Test
    public void testInsertExists() {
        // ctx1.setOldData(new String[] { "k3", "k4", "0.0", "0.0", "0", "0.0",
        // "0", "0.0", "5" });
        boolean result = filter
                .filterInsert(ctx1, new String[] { "k3", "k4", "1", "0.0", "2", "3.5", "4", "0.0", "5" });

        Assert.assertFalse(result);

        JdbcTemplate jdbcTemplate = getJdbcTemplate();

        StringBuilder verifySql = new StringBuilder();
        verifySql.append("select * from ");
        verifySql.append(TABLE_TEST_1);
        verifySql.append(" where PK1=? and PK2=?");

        jdbcTemplate.query(verifySql.toString(), new Object[] { "k3", "k4" }, new RowMapper<Object>() {

            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                Assert.assertEquals(rs.getDouble("add1"), 2.0, 0.0001);
                Assert.assertEquals(rs.getDouble("add2"), 2.0, 0.0001);
                Assert.assertEquals(rs.getDouble("add3"), 5.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr1"), 3.5, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr2"), 4.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr3"), 0, 0.0001);
                Assert.assertEquals(rs.getInt("nada1"), 7, 0.0001);
                return null;
            }

        });

        // Sequence 2

        ctx1.setOldData(new String[] { "k3", "k4", "1.0", "0.0", "0", "0.0", "0", "0.0", "5" });
        result = filter.filterInsert(ctx1, new String[] { "k3", "k4", "0", "0.1", "2", "3.5", "5", "5", "5" });

        Assert.assertFalse(result);

        jdbcTemplate = getJdbcTemplate();

        verifySql = new StringBuilder();
        verifySql.append("select * from ");
        verifySql.append(TABLE_TEST_1);
        verifySql.append(" where pk1=? and pk2=?");

        jdbcTemplate.query(verifySql.toString(), new Object[] { "k3", "k4" }, new RowMapper<Object>() {

            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                Assert.assertEquals(rs.getDouble("add1"), 1.0, 0.0001);
                Assert.assertEquals(rs.getDouble("add2"), 2.1, 0.0001);
                Assert.assertEquals(rs.getDouble("add3"), 7.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr1"), 3.5, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr2"), 5.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ovr3"), 5, 0.0001);
                Assert.assertEquals(rs.getInt("nada1"), 7, 0.0001);
                return null;
            }

        });
    }

}