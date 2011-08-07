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

import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TransformDataExtractorTest extends AbstractDatabaseTest {

    private static final String SIMPLE = "SIMPLE";

    private DataExtractorContext dataExtractorContext;

    private TransformDataExtractor transformDataExtractor;

    public TransformDataExtractorTest() throws Exception {
    }

    @Before
    public void setUp() {
        transformDataExtractor = find("transformDataExtractor");
        dataExtractorContext = (DataExtractorContext) find("extractorContext");
    }

    @Test
    public void testSimpleTableMapping() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory(SIMPLE, "ID", "ID, S1, X, Y, Z, TOTAL");
        Data data = new Data(SIMPLE, DataEventType.INSERT, "1, ONE, X, Y, Z, 1000", null,
                triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S1_A,S2_A,DECIMAL_A,LONGSTRING_A", data.getTriggerHistory()
                .getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,ONE,CONSTANT,1000,ONE-1", data.getRowData());
    }

    @Test
    public void testSimpleTableAdditiveUpdateMapping() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory(SIMPLE, "ID", "ID, S1, X, Y, Z, TOTAL");
        Data data = new Data(SIMPLE, DataEventType.UPDATE, "1, ONE, X, Y, Z, 10", "1",
                triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        data.setOldData("1, ONE, X, Y, Z, 1");

        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S1_A,S2_A,DECIMAL_A,LONGSTRING_A", data.getTriggerHistory()
                .getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,ONE,CONSTANT,9,ONE-1", data.getRowData());
        // TODO: is it okay for old data to be null after transformation?

        data = new Data(SIMPLE, DataEventType.UPDATE, "1, ONE, X, Y, Z, 8", "1", triggerHistory,
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setOldData("1, ONE, X, Y, Z, 9");
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S1_A,S2_A,DECIMAL_A,LONGSTRING_A", data.getTriggerHistory()
                .getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,ONE,CONSTANT,-1,ONE-1", data.getRowData());
    }

    @Test
    public void testTwoTablesMappedToOneInsert() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory("SOURCE_1", "ID", "ID, S1, X, Y, Z");
        Data data = new Data("SOURCE_1", DataEventType.INSERT, "4, BAMBOO, X, Y, Z", null,
                triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S1_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("4,BAMBOO,CONSTANT", data.getRowData());

        triggerHistory = new TriggerHistory("SOURCE_2", "ID2", "ID2, S2");
        data = new Data("SOURCE_2", DataEventType.INSERT, "4, STATUS4", null, triggerHistory,
                TestConstants.TEST_CHANNEL_ID, null, null);
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("4,STATUS4", data.getRowData());
    }

    @Test
    public void testTwoTablesMappedToOneDeleteUpdates() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory("SOURCE_2", "ID2", "ID2, S2");
        Data data = new Data("SOURCE_2", DataEventType.DELETE, null, "4", triggerHistory,
                TestConstants.TEST_CHANNEL_ID, null, null);
        data.setOldData("4, STATUS_4");
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals(DataEventType.UPDATE, data.getEventType());
        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        // TODO: should not be S2_A
        Assert.assertEquals("ID_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        // TODO: should not be DELETED
        Assert.assertEquals("4,DELETED", data.getRowData());
    }

    @Test
    public void testTableLookup() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory("SOURCE_B", "ID", "ID, S1");
        Data data = new Data("SOURCE_B", DataEventType.INSERT, "9, X", null, triggerHistory,
                TestConstants.TEST_CHANNEL_ID, null, null);
        data = toData(transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID,
                dataExtractorContext));

        Assert.assertEquals("ID_B", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_B,S1_B", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_B", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("9,12", data.getRowData());
    }

    @Test
    public void testMultiplyRows() throws Exception {

    }

    protected Data toData(Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Data> list = (List<Data>) value;
            if (list.size() > 0) {
                return list.get(0);
            }
        }
        return null;
    }

    protected void debug(Data data) {
        System.out.println("type: " + data.getEventType());
        System.out.println("keys: " + data.getTriggerHistory().getPkColumnNames());
        System.out.println("columns: " + data.getTriggerHistory().getColumnNames());
        System.out.println("table: " + data.getTriggerHistory().getSourceTableName());
        System.out.println("table: " + data.getTableName());
        System.out.println("row data: " + data.getRowData());
        System.out.println("pk data: " + data.getPkData());
        System.out.println("old data: " + data.getOldData());
    }

}
