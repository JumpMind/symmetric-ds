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

package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;

/*
 * Background job that is responsible for checking on node health. It will
 * disable nodes that have been offline for a configurable period of time.
 */
public class WatchdogJob extends AbstractJob {

    private INodeService nodeService;

    private IClusterService clusterService;

    @Override
    public long doJob() throws Exception {
        if (clusterService.lock(ClusterConstants.WATCHDOG)) {
            synchronized (this) {
                try {
                    nodeService.checkForOfflineNodes();
                } finally {
                    clusterService.unlock(ClusterConstants.WATCHDOG);
                }
            }
        }
        return -1l;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public String getClusterLockName() {
        return ClusterConstants.WATCHDOG;
    }

    public boolean isClusterable() {
        return true;
    }
}