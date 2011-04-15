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

package org.jumpmind.symmetric.service.jmx;

import java.io.ByteArrayOutputStream;

import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for outgoing synchronization")
/**
 * 
 */
public class OutgoingManagementService {

    protected IStatisticManager statisticManager;
    
    protected IDataExtractorService dataExtractorService;

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
  
    @ManagedOperation(description = "Show a batch in SymmetricDS Data Format.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "batchId", description = "The batch ID to display") })
    public String showBatch(String batchId) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOutgoingTransport transport = new InternalOutgoingTransport(out,null);
        dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
        out.close();
        return out.toString();
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }
}