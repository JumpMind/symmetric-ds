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
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;


/*
 * Background job that is responsible for writing statistics to database tables.
 */
public class StatisticFlushJob extends AbstractJob {

    public StatisticFlushJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.stat.flush", true, engine.getParameterService().is("start.stat.flush.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        engine.getStatisticManager().flush();
        engine.getPurgeService().purgeStats(force);
    }
    
    public String getClusterLockName() {
        return ClusterConstants.STATISTICS;
    }
    
    public boolean isClusterable() {
        return false;
    }
}