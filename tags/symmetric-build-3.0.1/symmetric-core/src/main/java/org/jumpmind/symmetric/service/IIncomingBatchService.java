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


package org.jumpmind.symmetric.service;

import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.model.IncomingBatch;

/**
 * This service provides an API to access to the incoming batch table. 
 */
public interface IIncomingBatchService {

    public int countIncomingBatchesInError();
    
    public IncomingBatch findIncomingBatch(long batchId, String nodeId);

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows);

    public boolean acquireIncomingBatch(IncomingBatch batch);

    public void insertIncomingBatch(IncomingBatch batch);

    public int updateIncomingBatch(IncomingBatch batch);
    
    public List<Date> listIncomingBatchTimes(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, boolean ascending);
    
    public List<IncomingBatch> listIncomingBatches(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime, int maxRowsToRetrieve, boolean ascending);

    public void markIncomingBatchesOk(String nodeId);
    
    public List<IncomingBatch> listIncomingBatchesInErrorFor(String nodeId);

}