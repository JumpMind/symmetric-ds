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

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TransformDataExtractorTest extends AbstractDataLoaderTest {

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
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);
        
        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        // TODO: ID_A is the PK, so it should be the first column listed
        Assert.assertEquals("ID_A,DECIMAL_A,LONGSTRING_A,S1_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,1000,ONE-1,ONE,CONSTANT", data.getRowData());
    }

    @Test
    public void testSimpleTableAdditiveUpdateMapping() throws Exception {    
        TriggerHistory triggerHistory = new TriggerHistory(SIMPLE, "ID", "ID, S1, X, Y, Z, TOTAL");
        Data data = new Data(SIMPLE, DataEventType.UPDATE, "1, ONE, X, Y, Z, 10", "1",
            triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        data.setOldData("1, ONE, X, Y, Z, 1");
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        // TODO: ID_A is the PK, so it should be the first column listed
        Assert.assertEquals("ID_A,DECIMAL_A,LONGSTRING_A,S1_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,9,ONE-1,ONE,CONSTANT", data.getRowData());
        // TODO: is it okay for old data to be null after transformation?

        data = new Data(SIMPLE, DataEventType.UPDATE, "1, ONE, X, Y, Z, 8", "1",
            triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        data.setOldData("1, ONE, X, Y, Z, 9");
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        // TODO: ID_A is the PK, so it should be the first column listed
        Assert.assertEquals("ID_A,DECIMAL_A,LONGSTRING_A,S1_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("1,-1,ONE-1,ONE,CONSTANT", data.getRowData());
    }

    @Test
    public void testTwoTablesMappedToOneInsert() throws Exception {
        TriggerHistory triggerHistory = new TriggerHistory("SOURCE_1", "ID", "ID, S1, X, Y, Z");
        Data data = new Data("SOURCE_1", DataEventType.INSERT, "4, BAMBOO, X, Y, Z", null,
            triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);

        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S1_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("4,BAMBOO,CONSTANT", data.getRowData());
        
        triggerHistory = new TriggerHistory("SOURCE_2", "ID2", "ID2, S2");
        data = new Data(SIMPLE, DataEventType.INSERT, "4, STATUS4", null,
            triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);
        
        Assert.assertEquals("ID_A", data.getTriggerHistory().getPkColumnNames());
        Assert.assertEquals("ID_A,S2_A", data.getTriggerHistory().getColumnNames());
        Assert.assertEquals("TEST_TRANSFORM_A", data.getTriggerHistory().getSourceTableName());
        Assert.assertEquals("4,STATUS4", data.getRowData());
    }

    @SuppressWarnings("unused")
    private void debug(Data data) {
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
