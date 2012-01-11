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

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for updating this node's heart beat time.
 */
public class HeartbeatJob extends AbstractJob {

    public HeartbeatJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.heartbeat", false, engine.getParameterService().is("start.heartbeat.job"),
                engine, taskScheduler);
    }

    @Override
    public long doJob() throws Exception {
        if (engine.getClusterService().lock(getClusterLockName())) {
            try {
                engine.getDataService().heartbeat(false);
                return -1l;
            } finally {
                engine.getClusterService().unlock(getClusterLockName());
            }
        } else {
            return -1l;
        }
    }

    public String getClusterLockName() {
        return ClusterConstants.HEARTBEAT;
    }

    public boolean isClusterable() {
        return true;
    }

}