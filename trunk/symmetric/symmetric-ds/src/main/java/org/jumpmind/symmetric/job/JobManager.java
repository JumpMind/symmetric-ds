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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.RandomTimeSlot;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class JobManager implements IJobManager {

    final ILog log = LogFactory.getLog(JobManager.class);

    private List<AbstractJob> jobs;

    private Map<String, ScheduledFuture<?>> startedJobs = new HashMap<String, ScheduledFuture<?>>();

    private IParameterService parameterService;

    private ThreadPoolTaskScheduler taskScheduler;

    private RandomTimeSlot randomTimeSlot;

    private void startJob(String name) {
        AbstractJob job = getJob(name);
        if (job != null) {
            log.info("JobStarting", name);
            String cronExpression = parameterService.getString(name + ".cron", null);
            if (!StringUtils.isBlank(cronExpression)) {
                startedJobs.put(name, taskScheduler.schedule(job, new CronTrigger(cronExpression)));
                job.setStarted(true);
                job.setCronExpression(cronExpression);
            } else {
                int period = parameterService.getInt(name + ".period.time.ms", -1);
                int startDelay = randomTimeSlot.getRandomValueSeededByDomainId();
                if (period > 0) {
                    startedJobs.put(name, taskScheduler.scheduleWithFixedDelay(job, new Date(System.currentTimeMillis()
                            + startDelay), period));
                    job.setStarted(true);
                    job.setTimeBetweenRunsInMs(period);
                } else {
                    log.error("JobFailedToSchedule", name);
                }
            }
        } else {
            log.error("JobFailedToFind", name);
        }
    }
    
    private AbstractJob getJob(String name) {
        for (AbstractJob job : jobs) {
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
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_PUSH_JOB))) {
            startJob(Constants.PUSH_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_PULL_JOB))) {
            startJob(Constants.PULL_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_ROUTE_JOB))) {
            startJob(Constants.ROUTE_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_PURGE_JOB))) {
            startJob(Constants.PURGE_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_HEARTBEAT_JOB))) {
            startJob(Constants.HEARTBEAT_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_SYNCTRIGGERS_JOB))) {
            startJob(Constants.SYNC_TRIGGERS_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_STATISTIC_FLUSH_JOB))) {
            startJob(Constants.STATISTIC_FLUSH_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_WATCHDOG_JOB))) {
            startJob(Constants.WATCHDOG_JOB_TIMER);
        }

    }

    public synchronized void stopJobs() {
        for (String jobName : startedJobs.keySet()) {
            ScheduledFuture<?> ref = startedJobs.get(jobName);
            if (ref.cancel(true)) {
                log.info("JobCancelled", jobName);
            } else {
                log.warn("JobFailedToCancel", jobName);
            }
        }        
    }
    
    public synchronized void destroy () {
        stopJobs();
        taskScheduler.destroy();
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    public void setRandomTimeSlot(RandomTimeSlot randomTimeSlot) {
        this.randomTimeSlot = randomTimeSlot;
    }

    public void setJobs(List<AbstractJob> jobs) {
        this.jobs = jobs;
    }

}