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
package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;

/**
 * Background job that pulls data from remote nodes and then loads it.
 */
public class PullJob extends AbstractJob {

    private IPullService pullService;
    
    private INodeService nodeService;

    @Override
    public long doJob() throws Exception {
        RemoteNodeStatuses statuses = pullService.pullData();

        // Re-pull immediately if we are in the middle of an initial load
        // so that the initial load completes as quickly as possible.
        // only process
        while (nodeService.isDataLoadStarted() &&
                !statuses.errorOccurred() && 
                statuses.wasBatchProcessed()) {
            log.info("DataPullingInReloadMode");
            statuses = pullService.pullData();
        }
        
        return statuses.getDataProcessedCount();
    }
    
    public String getClusterLockName() {
        return ClusterConstants.PULL;
    }
    
    public boolean isClusterable() {
        return true;
    }

    public void setPullService(IPullService service) {
        this.pullService = service;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}