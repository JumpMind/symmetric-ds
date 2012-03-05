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
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Before;
import org.junit.Test;

public class TransformDataLoaderTest extends AbstractDataLoaderTest {

    private static final String SIMPLE = "SIMPLE";

    public TransformDataLoaderTest() throws Exception {
    }
    
    @Before
    public void setup() {
        getJdbcTemplate().update("update sym_transform_table set transform_point='LOAD'");
        getSymmetricEngine().getTransformService().resetCache();
    }

    @Test
    public void testSimpleTableMapping() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getSimpleTransformCsv(), null, dl, null);
        expectCount(1, getTestTransform("A"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 1, "ONE", "CONSTANT"));
    }
    
    @Test
    public void testSimpleTableAdditiveUpdateMapping() throws Exception {
        testSimpleTableMapping();
        Assert.assertEquals(1000,
                getJdbcTemplate().queryForInt(getSelectDecimalFromA(), 1));
        TransformDataLoader dl = getTransformDataLoader();
        load(getSimpleTransformWithAdditiveUpdateCsv("1", "10"), null, dl, null);
        expectCount(1, getTestTransform("A"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 1, "ONE", "CONSTANT"));
        Assert.assertEquals(1009,
                getJdbcTemplate().queryForInt(getSelectDecimalFromA(), 1));
        
        load(getSimpleTransformWithAdditiveUpdateCsv("9", "8"), null, dl, null);
        Assert.assertEquals(1008,
                getJdbcTemplate().queryForInt(getSelectDecimalFromA(), 1));

    }
    
    @Test
    public void testSimpleTableBeanShellMapping() throws Exception {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        testSimpleTableMapping();
        Assert.assertEquals("ONE-1",
                getJdbcTemplate().queryForObject("select " + quote + "LONGSTRING_A" + quote + " from " + quote + "TEST_TRANSFORM_A" + quote + " where " + quote + "ID_A" + quote + "=?", String.class, 1));
    }    

    @Test
    public void testTwoTablesMappedToOneInsert() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getTwoTablesMappedToOneInsertCsv(), null, dl, null);
        expectCount(2, getTestTransform("A"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 4, "BAMBOO", "STATUS_4"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 5, "NUGGEE", "STATUS_5"));
    }

    @Test
    public void testTwoTablesMappedToOneDeleteUpdates() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        load(getTwoTablesMappedToOneInsertCsv(), null, dl, null);
        expectCount(2, getTestTransform("A"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 4, "BAMBOO", "STATUS_4"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 5, "NUGGEE", "STATUS_5"));
        load(getTwoTablesMappedToOneDeleteCsv(), null, dl, null);
        expectCount(2, getTestTransform("A"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 4, "BAMBOO", "DELETED"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("A"), 5, "NUGGEE", "STATUS_5"));
    }
    
    @Test
    public void testFallbackToUpdateTransform() throws Exception {
        TransformDataLoader dl = getTransformDataLoader();
        expectCount(0, getTestTransform("D"));
        load(getInsertIntoSource3Csv("Insert Value 1"), null, dl, null);
        expectCount(1, getTestTransform("D"));
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("D"), 99, "ABC", "Insert Value 1"));
        Map<String,Object> results = getTransformTableRow("D", 99);
        Assert.assertEquals("1", results.get("BIGINT_D").toString());
        Assert.assertEquals(null, results.get("LONGSTRING_D"));
        getJdbcTemplate().update(updateBigInt("D"), 0l);
        results = getTransformTableRow("D", 99);
        Assert.assertEquals("0", results.get("BIGINT_D").toString());
        load(getInsertIntoSource3Csv("Insert Value 1"), null, dl, null);
        Assert.assertEquals(1,
                getJdbcTemplate().queryForInt(getVerifyTransformTable("D"), 99, "ABC", "Insert Value 1"));
        results = getTransformTableRow("D", 99);
        Assert.assertEquals("0", results.get("BIGINT_D").toString());
        Assert.assertEquals("Updated", results.get("LONGSTRING_D"));

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
        writeTable(writer, SIMPLE, new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
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
        writeTable(writer, SIMPLE, new String[] { "ID" }, new String[] { "ID", "S1", "X", "Y",
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
    
    private ByteArrayOutputStream getInsertIntoSource3Csv(String sourceString) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, "SOURCE_3", new String[] { "ID" }, new String[] { "ID", "S1", "S2" });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "99", "ABC", sourceString }, true);
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
    
    protected String getVerifyTransformTable(String tableLetter) {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        return "select count(*) from " + quote + "TEST_TRANSFORM_" + tableLetter + quote + " where " + quote + "ID_" + tableLetter + quote + "=? and " + quote + "S1_" + tableLetter + quote + "=? and " + quote + "S2_" + tableLetter + quote + "=?";
    }
    
    protected Map<String,Object> getTransformTableRow(String tableLetter, int id) {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        return getJdbcTemplate().queryForMap("select * from " + quote + "TEST_TRANSFORM_" + tableLetter + quote + " where " + quote + "ID_" + tableLetter + quote + "=?", id);
    }
    
    protected String updateBigInt(String letter) {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        return "update " + quote + "TEST_TRANSFORM_" + letter + quote + " set " + quote + "BIGINT_" + letter + quote + "=?";
    }
    
    protected String getSelectDecimalFromA() {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        return "select " + quote + "DECIMAL_A" + quote + " from " + quote + "TEST_TRANSFORM_A" + quote + " where " + quote + "ID_A" + quote + "=?";
    }
    
    protected String getTestTransform(String letter) {
        String quote = getDbDialect().getPlatform().isDelimitedIdentifierModeOn() ? getDbDialect().getPlatform()
                .getPlatformInfo().getDelimiterToken() : "";
        return quote + "TEST_TRANSFORM_" + letter + quote;
    }

    @Before
    public void cleanSlate() {
        getDbDialect().truncateTable("TEST_TRANSFORM_A");
        getDbDialect().truncateTable("TEST_TRANSFORM_B");
        getDbDialect().truncateTable("TEST_TRANSFORM_C");
    }
}