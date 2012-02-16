package org.jumpmind.symmetric.io.data.writer;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.AbstractDbTest;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;
import org.junit.Assert;

abstract public class AbstractWriterTest extends AbstractDbTest {

    protected static IDatabasePlatform platform;
    
    protected boolean errorExpected = true;

    protected final static String TEST_TABLE = "test_dataloader_table";

    protected final static String[] TEST_KEYS = { "id" };

    protected final static String[] TEST_COLUMNS = { "id", "string_value", "string_required_value",
            "char_value", "char_required_value", "date_value", "time_value", "boolean_value",
            "integer_value", "decimal_value", "double_value" };

    protected static long batchId = 10000;

    protected static long sequenceId = 10000;

    protected synchronized long getNextBatchId() {
        return ++batchId;
    }

    protected synchronized long getBatchId() {
        return batchId;
    }

    protected synchronized String getNextId() {
        return String.valueOf(++sequenceId);
    }

    protected synchronized String getId() {
        return String.valueOf(sequenceId);
    }

    protected Table buildSourceTable(String tableName, String[] keyNames, String[] columnNames) {
        return Table.buildTable(tableName, keyNames, columnNames);
    }

    protected void writeData(CsvData data, String[] expectedValues) {
        writeData(data, expectedValues, TEST_COLUMNS);
    }

    protected void writeData(CsvData data, String[] expectedValues, String[] columnNames)
             {
        writeData(data, expectedValues, TEST_TABLE, TEST_KEYS, columnNames);
    }

    protected void writeData(CsvData data, String[] expectedValues, String tableName,
            String[] keyNames, String[] columnNames)  {
        Table table = buildSourceTable(tableName, keyNames, columnNames);
        writeData(new TableCsvData(table, data));
        String[] pkData = data.getParsedData(CsvData.ROW_DATA);
        if (pkData == null) {
            pkData = data.getParsedData(CsvData.PK_DATA);
        }
        assertTestTableEquals(pkData[0], expectedValues);
    }

    protected long writeData(TableCsvData... datas) {
        return writeData(new DatabaseWriter(platform), datas);
    }

    protected long writeData(IDataWriter writer, TableCsvData... datas) {
        DataContext context = new DataContext(
                null, writer);
        writer.open(context);
        try {
            for (TableCsvData tableCsvData : datas) {
                Batch batch = new Batch(getNextBatchId(), "default", BinaryEncoding.BASE64, "00001");
                try {
                    writer.start(batch);
                    if (writer.start(tableCsvData.table)) {
                        for (CsvData d : tableCsvData.data) {
                            writer.write(d);
                        }
                        writer.end(tableCsvData.table);
                    }
                    writer.end(batch, false);
                } catch (Exception ex) {
                    writer.end(batch, true);
                    if (!isErrorExpected()) {
                        if (ex instanceof RuntimeException) {
                            throw (RuntimeException)ex;
                        } else {
                            throw new RuntimeException(ex);
                        }
                    }

                }

            }
        } finally {
            writer.close();
        }

        long statementCount = 0;
        Collection<Statistics> stats = writer.getStatistics().values();
        for (Statistics statistics : stats) {
            statementCount += statistics.get(DataWriterStatisticConstants.STATEMENTCOUNT);
        }
        return statementCount;
    }

    protected void assertTestTableEquals(String testTableId, String[] expectedValues) {
        String sql = "select " + getSelect(TEST_COLUMNS) + " from " + TEST_TABLE + " where "
                + getWhere(TEST_KEYS);
        Map<String, Object> results = platform.getSqlTemplate().queryForMap(sql,
                new Object[] { new Long(testTableId) });

        if (expectedValues != null) {
            expectedValues[1] = translateExpectedString(expectedValues[1], false);
            expectedValues[2] = translateExpectedString(expectedValues[2], true);
            expectedValues[3] = translateExpectedCharString(expectedValues[3], 50, false);
            expectedValues[4] = translateExpectedCharString(expectedValues[4], 50, true);
        }
        assertEquals(TEST_COLUMNS, expectedValues, results);
    }

    protected String getSelect(String[] columns) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            str.append(columns[i]).append(i + 1 < columns.length ? ", " : "");
        }
        return str.toString();
    }

    protected String getWhere(String[] columns) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            str.append(columns[i]).append(" = ?").append(i + 1 < columns.length ? "," : "");
        }
        return str.toString();
    }

    protected String translateExpectedString(String value, boolean isRequired) {
        if (isRequired
                && (value == null || (value.equals("") && platform.getPlatformInfo()
                        .isEmptyStringNulled()))) {
            return AbstractDatabasePlatform.REQUIRED_FIELD_NULL_SUBSTITUTE;
        } else if (value != null && value.equals("")
                && platform.getPlatformInfo().isEmptyStringNulled()) {
            return null;
        }
        return value;
    }

    protected String translateExpectedCharString(String value, int size, boolean isRequired) {
        if (isRequired && value == null) {
            value = AbstractDatabasePlatform.REQUIRED_FIELD_NULL_SUBSTITUTE;
        }
        if (value != null
                && ((StringUtils.isBlank(value) && platform.getPlatformInfo()
                        .isBlankCharColumnSpacePadded()) || (StringUtils.isNotBlank(value) && platform
                        .getPlatformInfo().isNonBlankCharColumnSpacePadded()))) {
            return StringUtils.rightPad(value, size);
        } else if (value != null && platform.getPlatformInfo().isCharColumnSpaceTrimmed()) {
            return value.replaceFirst(" *$", "");
        }
        return value;
    }

    protected void assertEquals(String[] name, String[] expected, Map<String, Object> results) {
        if (expected == null) {
            Assert.assertNull("Expected empty results. " + printDatabase(), results);
        } else {
            Assert.assertNotNull("Expected non-empty results. " + printDatabase(), results);
            for (int i = 0; i < expected.length; i++) {
                Object resultObj = results.get(name[i]);
                String resultValue = null;
                char decimal = ((DecimalFormat) DecimalFormat.getInstance())
                        .getDecimalFormatSymbols().getDecimalSeparator();
                if (resultObj instanceof BigDecimal && expected[i].indexOf(decimal) != -1) {
                    DecimalFormat df = new DecimalFormat("0.00####################################");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Date) {
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Boolean) {
                    resultValue = ((Boolean) resultObj) ? "1" : "0";
                } else if (resultObj instanceof Double) {
                    resultValue = resultObj.toString();
                } else if (resultObj != null) {
                    resultValue = resultObj.toString();
                }

                Assert.assertEquals(name[i] + ". " + printDatabase(), expected[i], resultValue);
            }
        }
    }

    protected String printDatabase() {
        return " The database we are testing against is " + platform.getName() + ".";
    }
    
    protected boolean isOracle() {
        return DatabaseNamesConstants.ORACLE.equals(platform.getName());
    }
    
    public void setErrorExpected(boolean errorExpected) {
        this.errorExpected = errorExpected;
    }
    
    public boolean isErrorExpected() {
        return errorExpected;
    }

    class TableCsvData {
        Table table;
        List<CsvData> data;

        public TableCsvData(Table table, CsvData... csvDatas) {
            this.table = table;
            this.data = new ArrayList<CsvData>();
            for (CsvData csvData : csvDatas) {
                this.data.add(csvData);
            }
        }

        public TableCsvData(Table table, List<CsvData> data) {
            this.table = table;
            this.data = data;
        }

    }

}
