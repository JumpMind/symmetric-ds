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
package org.jumpmind.symmetric.service.impl;

import java.util.List;

import junit.framework.Assert;

import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Test;

public class IncomingBatchServiceTest extends AbstractDatabaseTest {


    public IncomingBatchServiceTest() throws Exception {
        super();
    }
    
    @Test
    public void testInsertAndUpdateIncomingBatchMaxSize() {        
        IncomingBatch batch = new IncomingBatch();
        batch.setStatus(Status.ER);
        batch.setNodeId("XXXXX");
        batch.setChannelId(TestConstants.TEST_CHANNEL_ID);
        batch.setByteCount(Long.MAX_VALUE);
        batch.setDatabaseMillis(Long.MAX_VALUE);
        batch.setFailedRowNumber(Long.MAX_VALUE);
        batch.setFallbackInsertCount(Long.MAX_VALUE);
        batch.setFilterMillis(Long.MAX_VALUE);
        batch.setMissingDeleteCount(Long.MAX_VALUE);
        batch.setNetworkMillis(Long.MAX_VALUE);
        batch.setSkipCount(Long.MAX_VALUE);
        batch.setStatementCount(Long.MAX_VALUE);
        getIncomingBatchService().insertIncomingBatch(batch);
        List<IncomingBatch> batches = getIncomingBatchService().findIncomingBatchErrors(1);
        Assert.assertEquals(1, batches.size());
        batch.setBatchId(batches.get(0).getBatchId());
        batch.setStatus(Status.OK);
        getIncomingBatchService().updateIncomingBatch(batch);
    }


}