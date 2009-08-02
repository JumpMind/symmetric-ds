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
package org.jumpmind.symmetric.integrate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.integrate.IPublisher;
import org.jumpmind.symmetric.integrate.XmlPublisherDataLoaderFilter;
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
        XmlPublisherDataLoaderFilter filter = new XmlPublisherDataLoaderFilter();
        filter.setTimeStringGenerator(new XmlPublisherDataLoaderFilter.ITimeGenerator() {
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

        public void publish(ICacheContext context, String text) {
            this.output = text;
        }

        @Override
        public String toString() {
            return output;
        }
    }
}
