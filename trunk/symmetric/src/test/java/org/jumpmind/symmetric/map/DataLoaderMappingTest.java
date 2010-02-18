package org.jumpmind.symmetric.map;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.TokenConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;

import com.csvreader.CsvWriter;

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
        Map<String, IColumnFilter> filters = new HashMap<String, IColumnFilter>();
        filters.put(TEST_TABLE, filter);

        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                ADDITIONAL_COLUMN_VALUE));
    }
    
    @Test
    public void testAddMultipleConstantColumns() throws Exception {

        final String ADDITIONAL_COLUMN_VALUE_1 = "Hello Johnny";
        final Integer ADDITIONAL_COLUMN_VALUE_2 = 42;
        
        final String ASSERT_SQL = "select count(*) from " + TEST_TABLE
                + " where id=? and column1=? and column2=? and int1=?";

        cleanSlate();

        ByteArrayOutputStream out = getStandardCsv();

        AddColumnsFilter filter = new AddColumnsFilter();
        Map<String, Object> additionalColumns = new HashMap<String, Object>();
        additionalColumns.put("column2", ADDITIONAL_COLUMN_VALUE_1);
        additionalColumns.put("int1", ADDITIONAL_COLUMN_VALUE_2);
        filter.setAdditionalColumns(additionalColumns);
        filter.setTables(new String[] { TEST_TABLE });
        Map<String, IColumnFilter> filters = new HashMap<String, IColumnFilter>();
        filters.put(TEST_TABLE, filter);

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
        Map<String, IColumnFilter> filters = new HashMap<String, IColumnFilter>();
        filters.put(TEST_TABLE, filter);

        load(out, filters);

        Assert.assertEquals(1, getJdbcTemplate().queryForInt(ASSERT_SQL, "1", "1",
                TestConstants.TEST_CLIENT_EXTERNAL_ID));
    }

    private void cleanSlate() {
        getJdbcTemplate().update("delete from " + TEST_TABLE);
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
