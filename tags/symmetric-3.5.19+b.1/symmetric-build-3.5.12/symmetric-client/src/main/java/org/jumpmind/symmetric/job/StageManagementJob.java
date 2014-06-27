/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class StageManagementJob extends AbstractJob {

    private IStagingManager stagingManager;

    public StageManagementJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler,
            IStagingManager stagingManager) {
        super("job.stage.management", true, engine.getParameterService().is(
                "start.stage.management.job"), engine, taskScheduler);
        this.stagingManager = stagingManager;
    }

    public String getClusterLockName() {
        return ClusterConstants.STAGE_MANAGEMENT;
    }

    @Override
    void doJob(boolean force) throws Exception {
        if (stagingManager != null) {
            stagingManager.clean(engine.getParameterService()
                    .getLong(ParameterConstants.STREAM_TO_FILE_TIME_TO_LIVE_MS));
        }
    }

}
