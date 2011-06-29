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
package org.jumpmind.symmetric.transform;

import java.io.ByteArrayOutputStream;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class TransformDataLoaderTest extends AbstractDataLoaderTest {

    private static final String SELECT_DECIMAL_FROM_A = "select decimal_a from test_transform_a where id_a=?";
    private static final String VERIFY_TRANSFORM_A_TABLE = "select count(*) from test_transform_a where id_a=? and s1_a=? and s2_a=?";

    public TransformDataLoaderTest() throws Exception {
    }

    @Test
    public void testSimpleTableMapping() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getSimpleTransformCsv(), null, dl, dl);
        expectCount(1, "test_transform_a");
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 1, "ONE", "CONSTANT"));
    }
    
    @Test
    public void testSimpleTableAdditiveUpdateMapping() throws Exception {
        testSimpleTableMapping();
        Assert.assertEquals(1000,
                getJdbcTemplate().queryForInt(SELECT_DECIMAL_FROM_A, 1));
        TransformDataLoader dl = getTransformDataLoader();
        load(getSimpleTransformWithAdditiveUpdateCsv("1", "10"), null, dl, dl);
        expectCount(1, "test_transform_a");
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 1, "ONE", "CONSTANT"));
        Assert.assertEquals(1009,
                getJdbcTemplate().queryForInt(SELECT_DECIMAL_FROM_A, 1));
        
        load(getSimpleTransformWithAdditiveUpdateCsv("9", "8"), null, dl, dl);
        Assert.assertEquals(1008,
                getJdbcTemplate().queryForInt(SELECT_DECIMAL_FROM_A, 1));

    }

    @Test
    public void testTwoTablesMappedToOneInsert() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getTwoTablesMappedToOneInsertCsv(), null, dl, dl);
        expectCount(2, "test_transform_a");
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 4, "BAMBOO", "STATUS_4"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 5, "NUGGEE", "STATUS_5"));
    }

    @Test
    public void testTwoTablesMappedToOneDeleteUpdates() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getTwoTablesMappedToOneInsertCsv(), null, dl, dl);
        expectCount(2, "test_transform_a");
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 4, "BAMBOO", "STATUS_4"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 5, "NUGGEE", "STATUS_5"));
        load(getTwoTablesMappedToOneDeleteCsv(), null, dl, dl);
        expectCount(2, "test_transform_a");
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 4, "BAMBOO", "DELETED"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(VERIFY_TRANSFORM_A_TABLE, 5, "NUGGEE", "STATUS_5"));
    }

    protected void expectCount(int count, String table) {
        Assert.assertEquals(count,
                getJdbcTemplate().queryForInt(String.format("select count(*) from %s", table)));
    }

    private ByteArrayOutputStream getSimpleTransformCsv() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, "SIMPLE", new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
                "Z", "TOTAL" });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "1", "ONE", "X", "Y", "Z", "1000" }, true);
        writer.close();
        return out;
    }
    
    private ByteArrayOutputStream getSimpleTransformWithAdditiveUpdateCsv(String start, String end) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, "SIMPLE", new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
                "Z", "TOTAL" });
        writer.write(CsvConstants.OLD);
        writer.writeRecord(new String[] { "1", "ONE", "X", "Y", "Z", start }, true);

        writer.write(CsvConstants.UPDATE);
        writer.writeRecord(new String[] { "1", "ONE", "X", "Y", "Z", end, "1" }, true);
        writer.close();
        return out;
    }

    private ByteArrayOutputStream getTwoTablesMappedToOneInsertCsv() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, "SOURCE_1", new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
                "Z" });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "4", "BAMBOO", "X", "Y", "Z" }, true);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "5", "NUGGEE", "X", "Y", "Z" }, true);
        writeTable(writer, "SOURCE_2", new String[] { "ID2" }, new String[] { "ID2", "S2" });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "4", "STATUS_4" }, true);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "5", "STATUS_5" }, true);
        writer.close();
        return out;
    }

    private ByteArrayOutputStream getTwoTablesMappedToOneDeleteCsv() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, "SOURCE_2", new String[] { "ID2" }, new String[] { "ID2", "S2" });
        writer.write(CsvConstants.OLD);
        writer.writeRecord(new String[] { "4", "STATUS_4" }, true);
        writer.write(CsvConstants.DELETE);
        writer.writeRecord(new String[] { "4" }, true);
        writer.close();
        return out;
    }


    @Before
    public void cleanSlate() {
        getDbDialect().truncateTable("test_transform_a");
        getDbDialect().truncateTable("test_transform_b");
        getDbDialect().truncateTable("test_transform_c");
    }
}