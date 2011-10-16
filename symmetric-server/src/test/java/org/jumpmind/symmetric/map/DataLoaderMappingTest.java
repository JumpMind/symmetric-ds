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
package org.jumpmind.symmetric.map;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;


public class DataLoaderMappingTest extends AbstractDataLoaderTest {

    protected final static String TEST_TABLE = "test_column_mapping";

    protected final static String[] TEST_KEYS = { "id" };

    public DataLoaderMappingTest() throws Exception {
    }

    @Test
    public void testAddConstantColumn() throws Exception {

        final String ADDITIONAL_COLUMN_VALUE = "Hello Kitty";
        final String ASSERT_SQL = "select count(*) from " + TEST_TABLE
                + " where id=? and column1=? and column2=?";

        cleanSlate();

        ByteArrayOutputStream out = getStandardCsv();
        
        load(out);

        Assert.assertEquals(0, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                ADDITIONAL_COLUMN_VALUE));

        cleanSlate();

        AddColumnsFilter filter = new AddColumnsFilter();
        Map<String, Object> additionalColumns = new HashMap<String, Object>();
        additionalColumns.put("column2", ADDITIONAL_COLUMN_VALUE);
        filter.setAdditionalColumns(additionalColumns);
        filter.setTables(new String[] { TEST_TABLE });
        Map<String, List<IColumnFilter>> filters = createColumnFilterList(TEST_TABLE, filter);
        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                ADDITIONAL_COLUMN_VALUE));
    }
       
    @Test
    public void testAddMultipleConstantColumns() throws Exception {

        final String ADDITIONAL_COLUMN_VALUE_1 = "Hello Johnny";
        final Integer ADDITIONAL_COLUMN_VALUE_2 = 42;
        
        final String ASSERT_SQL = "select count(*) from " + TEST_TABLE
                + " where id=? and column1=? and column2=? and field1=?";

        cleanSlate();

        ByteArrayOutputStream out = getStandardCsv();

        AddColumnsFilter filter = new AddColumnsFilter();
        Map<String, Object> additionalColumns = new HashMap<String, Object>();
        additionalColumns.put("column2", ADDITIONAL_COLUMN_VALUE_1);
        additionalColumns.put("field1", ADDITIONAL_COLUMN_VALUE_2);
        filter.setAdditionalColumns(additionalColumns);
        filter.setTables(new String[] { TEST_TABLE });
        Map<String, List<IColumnFilter>> filters = createColumnFilterList(TEST_TABLE, filter);

        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                ADDITIONAL_COLUMN_VALUE_1, ADDITIONAL_COLUMN_VALUE_2));
    }

    @Test
    public void testAddColumnsFilterReferenceAnotherColumn() throws Exception {

        final String ADDITIONAL_COLUMN_VALUE_1 = "Hello Johnny";
        final Integer ADDITIONAL_COLUMN_VALUE_2 = 42;
        
        final String ASSERT_SQL = "select count(*) from " + TEST_TABLE
                + " where id=? and column1=? and column2=? and field1=? and another_id_column=id";

        cleanSlate();

        ByteArrayOutputStream out = getStandardCsv();

        AddColumnsFilter filter = new AddColumnsFilter();
        Map<String, Object> additionalColumns = new HashMap<String, Object>();
        additionalColumns.put("column2", ADDITIONAL_COLUMN_VALUE_1);
        additionalColumns.put("field1", ADDITIONAL_COLUMN_VALUE_2);
        additionalColumns.put("another_id_column", ":id");
        filter.setAdditionalColumns(additionalColumns);
        filter.setTables(new String[] { TEST_TABLE });
        Map<String, List<IColumnFilter>> filters = createColumnFilterList(TEST_TABLE, filter);

        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                ADDITIONAL_COLUMN_VALUE_1, ADDITIONAL_COLUMN_VALUE_2));
    }
    
    @Test
    public void testAddExternalIdColumn() throws Exception {

        final String ASSERT_SQL = "select count(*) from " + TEST_TABLE
                + " where id=? and column1=? and column2=?";

        cleanSlate();

        ByteArrayOutputStream out = getStandardCsv();

        AddColumnsFilter filter = new AddColumnsFilter();
        Map<String, Object> additionalColumns = new HashMap<String, Object>();
        additionalColumns.put("column2", TokenConstants.EXTERNAL_ID);
        filter.setAdditionalColumns(additionalColumns);
        filter.setTables(new String[] { TEST_TABLE });
        Map<String, List<IColumnFilter>> filters = createColumnFilterList(TEST_TABLE, filter);

        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                TestConstants.TEST_CLIENT_EXTERNAL_ID));
    }
    
    
    @Test
    public void testColumnFilterChangingColumnName() throws Exception {
        String tableName = "test_changing_column_name";
        String[] keys = { "id" };
        String[] columns = { "id", "test" };
        String[] values = { "1", "10" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, tableName, keys, columns);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.close();
        ChangeColumnsNamesFilter filter = new ChangeColumnsNamesFilter();
        Map<String, String> mapping = new HashMap<String, String>();
        mapping.put("id", "id1");
        filter.setColumnNameMapping(mapping);
        Map<String, List<IColumnFilter>> filters = createColumnFilterList(tableName, filter);
        load(out, filters);
        Assert.assertEquals(1, getJdbcTemplate().queryForInt("select count(*) from test_changing_column_name"));
    }

    private void cleanSlate() {
        getDbDialect().truncateTable(TEST_TABLE);
    }
    
    private ByteArrayOutputStream getStandardCsv() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, new String[] { "id", "column1" });

        writer.write(CsvConstants.INSERT);
        writer.writeRecord(new String[] { "1", "1" }, true);

        writer.close();
        return out;
    }

}