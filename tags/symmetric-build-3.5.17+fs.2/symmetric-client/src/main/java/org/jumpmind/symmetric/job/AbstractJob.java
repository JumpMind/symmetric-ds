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

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.RandomTimeSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

@ManagedResource(description = "The management interface for a job")
abstract public class AbstractJob implements Runnable, IJob {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String jobName;

    private boolean requiresRegistration = true;

    private boolean paused = false;

    private Date lastFinishTime;

    private boolean running = false;

    private long lastExecutionTimeInMs;

    private long totalExecutionTimeInMs;

    private long numberOfRuns;

    private boolean started;

    private boolean hasNotRegisteredMessageBeenLogged = false;

    private ThreadPoolTaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledJob;

    private RandomTimeSlot randomTimeSlot;

    private boolean autoStartConfigured;

    protected ISymmetricEngine engine;

    protected AbstractJob(String jobName, boolean requiresRegistration, boolean autoStartRequired,
            ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        this.engine = engine;
        this.taskScheduler = taskScheduler;
        this.jobName = jobName;
        this.requiresRegistration = requiresRegistration;
        this.autoStartConfigured = autoStartRequired;
        IParameterService parameterService = engine.getParameterService();
        this.randomTimeSlot = new RandomTimeSlot(parameterService.getExternalId(),
                parameterService.getInt(ParameterConstants.JOB_RANDOM_MAX_START_TIME_MS));
    }

    public boolean isAutoStartConfigured() {
        return autoStartConfigured;
    }

    public void start() {
        if (this.scheduledJob == null && engine != null
                && !engine.getClusterService().isInfiniteLocked(getClusterLockName())) {
            String cronExpression = engine.getParameterService().getString(jobName + ".cron", null);
            int timeBetweenRunsInMs = engine.getParameterService().getInt(
                    jobName + ".period.time.ms", -1);
            if (!StringUtils.isBlank(cronExpression)) {
                log.info("Starting {} with cron expression: {}", jobName, cronExpression);
                this.scheduledJob = taskScheduler.schedule(this, new CronTrigger(cronExpression));
                started = true;
            } else {
                int startDelay = randomTimeSlot.getRandomValueSeededByExternalId();
                long currentTimeMillis = System.currentTimeMillis();
                long lastRunTime = currentTimeMillis - timeBetweenRunsInMs;
                Lock lock = engine.getClusterService().findLocks().get(getClusterLockName());
                if (lock != null && lock.getLastLockTime() != null) {
                    long newRunTime = lock.getLastLockTime().getTime();
                    if (lastRunTime < newRunTime) {
                        lastRunTime = newRunTime;
                    }
                }
                Date firstRun = new Date(lastRunTime + timeBetweenRunsInMs + startDelay);
                log.info("Starting {} on periodic schedule: every {}ms with the first run at {}", new Object[] {jobName,
                        timeBetweenRunsInMs, firstRun});
                if (timeBetweenRunsInMs > 0) {
                    this.scheduledJob = taskScheduler.scheduleWithFixedDelay(this,
                            firstRun, timeBetweenRunsInMs);
                    started = true;
                } else {
                    log.error("Failed to schedule this job, {}", jobName);
                }
            }
        }
    }

    public boolean stop() {
        boolean success = false;
        if (this.scheduledJob != null) {
            success = this.scheduledJob.cancel(true);
            this.scheduledJob = null;
            if (success) {
                log.info("The {} job has been cancelled", jobName);
                started = false;
            } else {
                log.warn("Failed to cancel this job, {}", jobName);
            }
        }
        return success;
    }

    public String getName() {
        return jobName;
    }

    @ManagedOperation(description = "Run this job is it isn't already running")
    public boolean invoke() {
        return invoke(true);
    }

    public boolean invoke(boolean force) {
        IParameterService parameterService = engine.getParameterService();
        boolean ran = false;
        try {
            if (engine == null) {
                log.info("Could not find a reference to the SymmetricEngine from {}", jobName);
            } else {
                if (!Thread.interrupted()) {
                    MDC.put("engineName", engine.getEngineName());
                    if (engine.isStarted()) {
                        if (!paused || force) {
                            if (!running) {
                                running = true;
                                synchronized (this) {
                                    ran = true;
                                    long startTime = System.currentTimeMillis();
                                    try {
                                        if (!requiresRegistration
                                                || (requiresRegistration && engine
                                                        .getRegistrationService()
                                                        .isRegisteredWithServer())) {
                                            hasNotRegisteredMessageBeenLogged = false;
                                           if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)) {
                                                synchronized (AbstractJob.class) {
                                                    doJob(force);
                                                }
                                            } else {
                                                doJob(force);
                                            }
                                        } else {
                                            if (!hasNotRegisteredMessageBeenLogged) {
                                                log.warn(
                                                        "Did not run the {} job because the engine is not registered.",
                                                        getName());
                                                hasNotRegisteredMessageBeenLogged = true;
                                            }
                                        }
                                    } finally {
                                        lastFinishTime = new Date();
                                        long endTime = System.currentTimeMillis();
                                        lastExecutionTimeInMs = endTime - startTime;
                                        totalExecutionTimeInMs += lastExecutionTimeInMs;
                                        if (lastExecutionTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
                                            engine.getStatisticManager().addJobStats(jobName,
                                                    startTime, endTime, 0);
                                        }
                                        numberOfRuns++;
                                        running = false;
                                    }
                                }
                            }
                        }
                    } else {
                        log.info("The engine is not currently started.");
                    }
                } else {
                    log.warn("This thread was interrupted.  Not executing the job until the interrupted status has cleared");
                }
            }
        } catch (final Throwable ex) {
            log.error(ex.getMessage(), ex);
        }

        return ran;
    }

    /*
     * This method is called from the job
     */
    public void run() {
        MDC.put("engineName", engine != null ? engine.getEngineName() : "unknown");
        invoke(false);
    }

    abstract void doJob(boolean force) throws Exception;

    @ManagedOperation(description = "Pause this job")
    public void pause() {
        setPaused(true);
    }

    @ManagedOperation(description = "Resume the job")
    public void unpause() {
        setPaused(false);
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    @ManagedAttribute(description = "If true, this job has been paused")
    public boolean isPaused() {
        return paused;
    }

    @ManagedAttribute(description = "If true, this job has been started")
    public boolean isStarted() {
        return started;
    }

    @ManagedMetric(description = "The amount of time this job spent in execution during it's last run")
    public long getLastExecutionTimeInMs() {
        return lastExecutionTimeInMs;
    }

    @ManagedAttribute(description = "The last time this job completed execution")
    public Date getLastFinishTime() {
        return lastFinishTime;
    }

    @ManagedAttribute(description = "If true, the job is already running")
    public boolean isRunning() {
        return running;
    }

    @ManagedMetric(description = "The number of times this job has been run during the lifetime of the JVM")
    public long getNumberOfRuns() {
        return numberOfRuns;
    }

    @ManagedMetric(description = "The total amount of time this job has spent in execution during the lifetime of the JVM")
    public long getTotalExecutionTimeInMs() {
        return totalExecutionTimeInMs;
    }

    @ManagedMetric(description = "The total amount of time this job has spend in execution during the lifetime of the JVM")
    public long getAverageExecutionTimeInMs() {
        if (numberOfRuns > 0) {
            return totalExecutionTimeInMs / numberOfRuns;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "If set, this is the cron expression that governs when the job will run")
    public String getCronExpression() {
        return engine.getParameterService().getString(jobName + ".cron", null);
    }

    @ManagedAttribute(description = "If the cron expression isn't set.  This is the amount of time that will pass before the periodic job runs again.")
    public long getTimeBetweenRunsInMs() {
        return engine.getParameterService().getInt(jobName + ".period.time.ms", -1);
    }

}
