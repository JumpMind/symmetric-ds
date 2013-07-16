package org.jumpmind.symmetric.model;

import junit.framework.Assert;

import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.junit.Test;

public class CsvDataTest {

    @Test
    public void testGetCsvData() {
        final String TEST = "\"This is a test\", laughs Kunal.\n\r";
        CsvData data = new CsvData(DataEventType.INSERT, new String[] {TEST});
        String rowData = data.getCsvData(CsvData.ROW_DATA);
        CsvData newData = new CsvData();
        newData.putCsvData(CsvData.ROW_DATA, rowData);
        String result = newData.getParsedData(CsvData.ROW_DATA)[0];
        Assert.assertEquals(TEST, result);
    }
}
