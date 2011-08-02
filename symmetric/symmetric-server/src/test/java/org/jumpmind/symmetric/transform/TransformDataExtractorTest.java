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

import java.util.Date;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.test.TestConstants;
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
        Data data = new Data(1, "328", "1, ONE, X, Y, Z, 1000", DataEventType.INSERT, SIMPLE, new Date(),
            triggerHistory, TestConstants.TEST_CHANNEL_ID, null, null);
        transformDataExtractor.filterData(data, Constants.UNKNOWN_ROUTER_ID, dataExtractorContext);        
        
        System.out.println("columns: " + data.getTriggerHistory().getColumnNames());
        System.out.println("row data: " + data.getRowData());
    }

}
