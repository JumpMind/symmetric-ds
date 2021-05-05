package org.jumpmind.symmetric.io.data.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataProcessor;
import org.junit.Test;

public class XmlDataReaderTest {

    @Test
    public void testNilDataElement() {
        XmlDataReader reader = new XmlDataReader(getClass().getResourceAsStream("xmldatareadertest1.xml"));
        TestableDataWriter writer = new TestableDataWriter();
        DataProcessor processor = new DataProcessor(reader, writer, "test");
        processor.process();
        List<CsvData> dataRead = writer.getDatas();
        assertEquals(4, dataRead.size());
        Map<String, String> data1 = dataRead.get(1).toColumnNameValuePairs(writer.getLastTableRead().getColumnNames(), CsvData.ROW_DATA);
        assertEquals("1", data1.get("id"));
        assertEquals("A", data1.get("my_value"));

        Map<String, String> data2 = dataRead.get(2).toColumnNameValuePairs(writer.getLastTableRead().getColumnNames(), CsvData.ROW_DATA);
        assertEquals("2", data2.get("id"));
        assertNull(data2.get("my_value"));

    }
}
