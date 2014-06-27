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

import java.util.List;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * @see IJobManager
 */
public class JobManager implements IJobManager {

    final ILog log = LogFactory.getLog(JobManager.class);

    private List<IJob> jobs;
    
    private ThreadPoolTaskScheduler taskScheduler;

    public IJob getJob(String name) {
        for (IJob job : jobs) {
            if (job.getName().equals(name)) {
                return job;
            }
        }
        return null;
    }
    
    /**
     * Start the jobs if they are configured to be started in
     * symmetric.properties
     */
    public synchronized void startJobs() {
        for (IJob job : jobs) {
            if (job.isAutoStartConfigured()) {
                job.start();
            } else {
                log.info("JobNoAutoStart", job.getName());
            }
        }
    }

    public synchronized void stopJobs() {
        for (IJob job : jobs) {
            job.stop();
        }      
    }
    
    public synchronized void destroy () {
        stopJobs();
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    public void setJobs(List<IJob> jobs) {
        this.jobs = jobs;
    }
    
    public List<IJob> getJobs() {
        return jobs;
    }

    public void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }
}