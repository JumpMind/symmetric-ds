/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.ext;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class AdditiveDataLoaderFilterTest extends AbstractDatabaseTest {

    private static final String TABLE_TEST_1 = "TEST_ADD_DL_TABLE_1";
    private static final String TABLE_TEST_2 = "TEST_ADD_DL_TABLE_2";

    private DataLoaderContext ctx1;
    private AdditiveDataLoaderFilter filter;
    private AdditiveDataLoaderFilter filter2;

    private DataLoaderContext ctx2;

    public AdditiveDataLoaderFilterTest() throws Exception {
        super();
    }

    public AdditiveDataLoaderFilterTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {

        // Context 1 is for a filtered table; context 2, non-filtered.

        ctx1 = new DataLoaderContext();
        ctx1.setNodeId("54321");
        ctx1.setTableName(TABLE_TEST_1);
        ctx1.setTableTemplate(new TableTemplate(getJdbcTemplate(), getDbDialect(), TABLE_TEST_1, null, false, null,
                null));
        ctx1.setColumnNames(new String[] { "PK1", "PK2", "ADD1", "ADD2", "ADD3", "OVR1", "OVR2", "OVR3", "NADA1" });
        ctx1.setKeyNames(new String[] { "PK1", "PK2" });

        ctx2 = new DataLoaderContext();
        ctx2.setNodeId("54321");
        ctx2.setTableName(TABLE_TEST_2);
        ctx2.setTableTemplate(new TableTemplate(getJdbcTemplate(), getDbDialect(), TABLE_TEST_2, null, false, null,
                null));
        ctx2.setKeyNames(new String[] { "PK1" });
        ctx2.setColumnNames(new String[] { "PK1", "ADD1" });

        filter = new AdditiveDataLoaderFilter();
        filter.setTableName(TABLE_TEST_1);
        filter.setJdbcTemplate(getJdbcTemplate());
        filter.setAdditiveColumnNames(new String[] { "ADD1", "ADD2", "ADD3" });
        filter.setOverrideColumnNames(new String[] { "OVR1", "OVR2", "OVR3" });

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

        jdbcTemplate.query(verifySql.toString(), new Object[] { "k3", "k4" }, new RowMapper() {

            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                Assert.assertEquals(rs.getDouble("ADD1"), 2.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ADD2"), 2.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ADD3"), 5.0, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR1"), 3.5, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR2"), 4.0, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR3"), 0, 0.0001);
                Assert.assertEquals(rs.getInt("NADA1"), 7, 0.0001);
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
        verifySql.append(" where PK1=? and PK2=?");

        jdbcTemplate.query(verifySql.toString(), new Object[] { "k3", "k4" }, new RowMapper() {

            public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                Assert.assertEquals(rs.getDouble("ADD1"), 1.0, 0.0001);
                Assert.assertEquals(rs.getDouble("ADD2"), 2.1, 0.0001);
                Assert.assertEquals(rs.getDouble("ADD3"), 7.0, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR1"), 3.5, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR2"), 5.0, 0.0001);
                Assert.assertEquals(rs.getDouble("OVR3"), 5, 0.0001);
                Assert.assertEquals(rs.getInt("NADA1"), 7, 0.0001);
                return null;
            }

        });
    }

}
