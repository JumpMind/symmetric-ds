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

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that pulls data from remote nodes and then loads it.
 */
public class PullJob extends AbstractJob {

    public PullJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.pull", false, engine.getParameterService().is("start.pull.job"),
                engine, taskScheduler);
    }
    
    @Override
    public long doJob() throws Exception {
        RemoteNodeStatuses statuses = engine.getPullService().pullData();

        // Re-pull immediately if we are in the middle of an initial load
        // so that the initial load completes as quickly as possible.
        // only process
        while (engine.getNodeService().isDataLoadStarted() &&
                !statuses.errorOccurred() && 
                statuses.wasBatchProcessed()) {
            log.info("Immediate pull requested while in reload mode");
            statuses = engine.getPullService().pullData();
        }
        
        return statuses.getDataProcessedCount();
    }
    
    public String getClusterLockName() {
        return ClusterConstants.PULL;
    }
    
    public boolean isClusterable() {
        return true;
    }

}
