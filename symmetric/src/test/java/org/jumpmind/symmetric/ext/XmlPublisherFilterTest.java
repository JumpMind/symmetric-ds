package org.jumpmind.symmetric.ext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.load.csv.CsvLoader;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class XmlPublisherFilterTest extends AbstractDatabaseTest {

    private static final String TABLE_TEST = "TEST_XML_PUBLISHER";
    
    private static final String TEST_SIMPLE_TRANSFORM_RESULTS = "<batch xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" id=\"12\" nodeid=\"54321\" time=\"test\"><row entity=\"TEST_XML_PUBLISHER\" dml=\"I\"><data key=\"ID1\">1</data><data key=\"ID2\">2</data><data key=\"DATA1\">test embedding an &amp;</data><data key=\"DATA2\">3</data><data key=\"DATA3\" xsi:nil=\"true\" /></row></batch>";

    private DataLoaderContext ctx;

    public XmlPublisherFilterTest() throws Exception {
        super();
    }

    public XmlPublisherFilterTest(String dbName) {
        super(dbName);
    }

    @Before
    public void setUp() {
        ctx = new DataLoaderContext();
        ctx.setNodeId("54321");
        ctx.setTableName(TABLE_TEST);
        ctx.setTableTemplate(new TableTemplate(getJdbcTemplate(), getDbDialect(), TABLE_TEST, null, false, null, null));
        ctx.setColumnNames(new String[] { "ID1", "ID2", "DATA1", "DATA2", "DATA3" });

    }

    @Test
    public void testSimpleTransform() {
        XmlPublisherFilter filter = new XmlPublisherFilter();
        filter.setTimeStringGenerator(new XmlPublisherFilter.ITimeGenerator() {
            public String getTime() {
                return "test";
            }
        });
        HashSet<String> tableNames = new HashSet<String>();
        tableNames.add(TABLE_TEST);
        filter.setTableNamesToPublishAsGroup(tableNames);
        List<String> columns = new ArrayList<String>();
        columns.add("ID1");
        columns.add("ID2");
        filter.setGroupByColumnNames(columns);
        Output output = new Output();
        filter.setPublisher(output);

        String[][] data = { { "1", "1", "The Angry Brown", "3", "2008-10-24 00:00:00.0" },
                { "1", "2", "test embedding an &", "3", null } };
        for (String[] strings : data) {
            filter.filterInsert(ctx, strings);
            filter.batchComplete(new CsvLoader() {
                @Override
                public IDataLoaderContext getContext() {
                    return ctx;
                }
            }, null);
        }

        Assert.assertEquals(TEST_SIMPLE_TRANSFORM_RESULTS.trim(), output.toString().trim());

    }

    class Output implements IPublisher {
        private String output;

        public void publish(IDataLoaderContext context, String text) {
            this.output = text;
        }

        @Override
        public String toString() {
            return output;
        }
    }
}
