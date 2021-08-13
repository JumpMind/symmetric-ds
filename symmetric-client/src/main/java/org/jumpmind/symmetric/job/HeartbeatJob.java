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

import static org.jumpmind.symmetric.job.JobDefaults.EVERY_FIFTEEN_MINUTES;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for updating this node's heart beat time.
 */
public class HeartbeatJob extends AbstractJob {
    public HeartbeatJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super(ClusterConstants.HEARTBEAT, engine, taskScheduler);
        if (engine.getParameterService().is(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER)) {
            engine.getExtensionService().addExtensionPoint(new OracleNoOrderHeartbeat(engine));
        }
    }

    @Override
    public JobDefaults getDefaults() {
        return new JobDefaults()
                .description("Record a heartbeat")
                .requiresRegisteration(false)
                .schedule(EVERY_FIFTEEN_MINUTES);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        if (engine.getClusterService().lock(getName())) {
            try {
                engine.getDataService().heartbeat(false);
            } finally {
                engine.getClusterService().unlock(getName());
            }
        }
    }
}