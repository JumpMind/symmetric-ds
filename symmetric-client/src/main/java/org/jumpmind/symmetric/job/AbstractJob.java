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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.JobDefinition;
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
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.SimpleTriggerContext;

@ManagedResource(description = "The management interface for a job")
abstract public class AbstractJob implements Runnable, IJob {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private String jobName;

    private JobDefinition jobDefinition;

    private AtomicBoolean paused = new AtomicBoolean(false);

    private Date lastFinishTime;

    private AtomicBoolean running = new AtomicBoolean(false);

    private long lastExecutionTimeInMs;

    private long totalExecutionTimeInMs;

    private long numberOfRuns;

    private boolean started;

    private boolean hasNotRegisteredMessageBeenLogged = false;

    protected ISymmetricEngine engine;

    private ThreadPoolTaskScheduler taskScheduler;

    private ScheduledFuture<?> scheduledJob;

    private RandomTimeSlot randomTimeSlot;
    
    private CronTrigger cronTrigger;
    
    private Date periodicFirstRunTime;
    
    private IParameterService parameterService;

    private long processedCount;
    
    private String targetNodeId;
    
    private int targetNodeCount;
    
    public AbstractJob() {
        
    }
    
    public AbstractJob(String jobName, ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        this.engine = engine;
        this.taskScheduler = taskScheduler;
        this.jobName = jobName;
        this.parameterService = engine.getParameterService();
        this.randomTimeSlot = new RandomTimeSlot(parameterService.getExternalId(),
                parameterService.getInt(ParameterConstants.JOB_RANDOM_MAX_START_TIME_MS));        
    }

    public void start() {
        if (this.scheduledJob == null && engine != null
                && !engine.getClusterService().isInfiniteLocked(getName())) {
            if (isCronSchedule()) {
                String cronExpression = getSchedule();
                cronTrigger = new CronTrigger(cronExpression);
                log.info("Starting job '{}' with cron expression: '{}'", jobName, cronExpression, cronTrigger.nextExecutionTime(new SimpleTriggerContext()));
                try {                    
                    this.scheduledJob = taskScheduler.schedule(this, cronTrigger);
                } catch (Exception ex) {
                    throw new SymmetricException("Failed to schedule job '" + jobName + "' with schedule '" + 
                            getSchedule() + "'", ex);
                }
                started = true;
            } else {
                long timeBetweenRunsInMs = getTimeBetweenRunsInMs();
                if (timeBetweenRunsInMs <= 0) {
                    return;
                }

                if (randomTimeSlot == null) {
                    this.randomTimeSlot = new RandomTimeSlot(parameterService.getExternalId(),
                            parameterService.getInt(ParameterConstants.JOB_RANDOM_MAX_START_TIME_MS));
                }
                int startDelay = randomTimeSlot.getRandomValueSeededByExternalId();
                long currentTimeMillis = System.currentTimeMillis();
                long lastRunTime = currentTimeMillis - timeBetweenRunsInMs;
                Lock lock = engine.getClusterService().findLocks().get(getName());
                if (lock != null && lock.getLastLockTime() != null) {
                    long newRunTime = lock.getLastLockTime().getTime();
                    if (lastRunTime < newRunTime) {
                        lastRunTime = newRunTime;
                    }
                }
                periodicFirstRunTime = new Date(lastRunTime + timeBetweenRunsInMs + startDelay);
                log.info("Starting {} on periodic schedule: every {}ms with the first run at {}", new Object[] {jobName,
                        timeBetweenRunsInMs, periodicFirstRunTime});
                this.scheduledJob = taskScheduler.scheduleWithFixedDelay(this,
                        periodicFirstRunTime, timeBetweenRunsInMs);
                started = true;
            }
        }
    }

    protected long getTimeBetweenRunsInMs() {
        long timeBetweenRunsInMs = -1;
        String schedule = getSchedule();
        
        try {
            if (StringUtils.isEmpty(schedule)) {
                throw new SymmetricException("Schedule value is not defined for " + jobDefinition.getPeriodicParameter());
            }
            timeBetweenRunsInMs = Long.parseLong(getSchedule());
            if (timeBetweenRunsInMs <= 0) {
                throw new SymmetricException("Schedule value must be positive, but was '" + schedule + "'");
            }
        } catch (Exception ex) {
            log.error("Failed to schedule job '" + jobName + "' because of an invalid schedule: '" + 
                    getSchedule() + "' Check the " + jobDefinition.getPeriodicParameter() + " parameter.", ex);
            return -1;
        }
        return timeBetweenRunsInMs;
    }

    public boolean stop() {
        boolean success = false;
        if (this.scheduledJob != null) {
            success = this.scheduledJob.cancel(true);
            this.scheduledJob = null;
            if (success) {
                log.info("The '{}' job has been cancelled", jobName);
                started = false;
            } else {
                log.warn("Failed to cancel the '{}' job", jobName);
            }
        }
        return success;
    }

    public String getName() {
        return jobName;
    }

    public JobDefinition getJobDefinition() {
        return jobDefinition;
    }

    public void setJobDefinition(JobDefinition jobDefinition) {
        this.jobDefinition = jobDefinition;
    }

    @ManagedOperation(description = "Run this job if it isn't already running")
    public boolean invoke() {
        return invoke(true);
    }

    @Override
    public boolean invoke(boolean force) {
        try {            
            MDC.put("engineName", engine.getEngineName());
            
            IParameterService parameterService = engine.getParameterService();
            long recordStatisticsCountThreshold = parameterService.getLong(ParameterConstants.STATISTIC_RECORD_COUNT_THRESHOLD,-1);
            
            boolean ok = checkPrerequsites(force);
            if (!ok) {
                return false;
            }
            
            long startTime = System.currentTimeMillis();
            try {
                if (!running.compareAndSet(false, true)) { // This ensures this job only runs once on this instance.
                    log.info("Job '{}' is already running on another thread and will not run at this time.", getName());
                    return false;
                }
                if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)) {
                    synchronized (AbstractJob.class) {
                        doJob(force);
                    }
                } else {
                    doJob(force);
                }

            } finally {
                lastFinishTime = new Date();
                long endTime = System.currentTimeMillis();
                lastExecutionTimeInMs = endTime - startTime;
                totalExecutionTimeInMs += lastExecutionTimeInMs;
                if (lastExecutionTimeInMs > Constants.LONG_OPERATION_THRESHOLD || 
                        (recordStatisticsCountThreshold > 0 && getProcessedCount() > recordStatisticsCountThreshold)) {
                    engine.getStatisticManager().addJobStats(targetNodeId, targetNodeCount, jobName,
                            startTime, endTime, getProcessedCount());
                }
                numberOfRuns++;
                running.set(false);
            }
        } catch (final Throwable ex) {
            log.error("Exception while executing job '" + getName() + "'", ex);
        } 

        return true;
    }

    /**
     * @return
     */
    protected boolean checkPrerequsites(boolean force) {
        if (engine == null) {
            log.info("Could not find a reference to the SymmetricEngine while running job '{}'", getName());
            return false;
        }
        if (Thread.interrupted()) {
            log.warn("This thread was interrupted.  Not executing the job '{}' until the interrupted status has cleared", getName());
            return false;
        }
        if (!engine.isStarted()) {
            log.info("The engine is not currently started, will not run job '{}'", getName());
            return false;
        }
        if (running.get()) {
            log.info("Job '{}' is already marked as running, will not run again now.", getName());
            return false;            
        }
        if (paused.get() && !force) {
            log.info("Job '{}' is paused and will not run at this time.", getName());
            return false;
        }
        if (jobDefinition.isRequiresRegistration() && !engine.getRegistrationService().isRegisteredWithServer()) {      
            if (!hasNotRegisteredMessageBeenLogged) {
                log.info("Did not run the '{}' job because the engine is not registered.", getName());
                hasNotRegisteredMessageBeenLogged = true;
            }
        }

        if (jobDefinition.getNodeGroupId() != null 
                && !jobDefinition.getNodeGroupId().equals("ALL") 
                && !jobDefinition.getNodeGroupId().equals(engine.getNodeService().findIdentity().getNodeGroupId())){
            log.info("Job should be only run on node groups '{}' but this is '{}'", 
                    jobDefinition.getNodeGroupId(), 
                    engine.getNodeService().findIdentity().getNodeGroupId());
            return false;
        }

        return true;
    }

    /*
     * This method is called from the job
     */
    public void run() {
        MDC.put("engineName", engine != null ? engine.getEngineName() : "unknown");
        invoke(false);
    }

    protected abstract void doJob(boolean force) throws Exception;

    @Override
    @ManagedOperation(description = "Pause this job")
    public void pause() {
        setPaused(true);
    }

    @Override
    @ManagedOperation(description = "Resume the job")
    public void unpause() {
        setPaused(false);
    }

    public void setPaused(boolean paused) {
        this.paused.set(paused);
    }

    @Override
    @ManagedAttribute(description = "If true, this job has been paused")
    public boolean isPaused() {
        return paused.get();
    }

    @Override
    @ManagedAttribute(description = "If true, this job has been started")
    public boolean isStarted() {
        return started;
    }

    @Override
    @ManagedMetric(description = "The amount of time this job spent in execution during it's last run")
    public long getLastExecutionTimeInMs() {
        return lastExecutionTimeInMs;
    }

    @Override
    @ManagedAttribute(description = "The last time this job completed execution")
    public Date getLastFinishTime() {
        return lastFinishTime;
    }

    @Override
    @ManagedAttribute(description = "If true, the job is already running")
    public boolean isRunning() {
        return running.get();
    }

    @Override
    @ManagedMetric(description = "The number of times this job has been run during the lifetime of the JVM")
    public long getNumberOfRuns() {
        return numberOfRuns;
    }

    @Override
    @ManagedMetric(description = "The total amount of time this job has spent in execution during the lifetime of the JVM")
    public long getTotalExecutionTimeInMs() {
        return totalExecutionTimeInMs;
    }
    
    @Override
    public Date getNextExecutionTime() {
        if (isCronSchedule() && cronTrigger != null) {
            return cronTrigger.nextExecutionTime(new TriggerContext() {
                
                @Override
                public Date lastScheduledExecutionTime() {
                    return null;
                }
                
                @Override
                public Date lastCompletionTime() {
                    return getLastFinishTime();
                }
                
                @Override
                public Date lastActualExecutionTime() {
                    return null;
                }
            });
        } else if (isPeriodicSchedule() ) {
            if (getLastFinishTime() != null) {
                return new Date(getLastFinishTime().getTime() + getTimeBetweenRunsInMs());
            } else if (periodicFirstRunTime != null) {
                return new Date(periodicFirstRunTime.getTime() + getTimeBetweenRunsInMs());
            } 
        }
        return null;
        
    }

    @Override
    @ManagedMetric(description = "The total amount of time this job has spend in execution during the lifetime of the JVM")
    public long getAverageExecutionTimeInMs() {
        if (numberOfRuns > 0) {
            return totalExecutionTimeInMs / numberOfRuns;
        } else {
            return 0;
        }
    }
    
    public boolean isCronSchedule() {
        return !isPeriodicSchedule();
    }
    
    public boolean isPeriodicSchedule() {
        String schedule = getSchedule();
        return NumberUtils.isDigits(schedule);
    }
    
    public String getSchedule() {
        String cronSchedule = parameterService.getString(jobDefinition.getCronParameter());
        if (!StringUtils.isEmpty(cronSchedule)) {
            return cronSchedule;
        } 
        String periodicSchedule = parameterService.getString(jobDefinition.getPeriodicParameter());
        if (!StringUtils.isEmpty(periodicSchedule)) {
            return periodicSchedule;            
        }
        
        return jobDefinition.getDefaultSchedule();
    }
    
    public abstract JobDefaults getDefaults();

    public ISymmetricEngine getEngine() {
        return engine;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

    public ThreadPoolTaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public void setTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }
    
    
    public long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(long processedCount) {
        this.processedCount = processedCount;
    }
    
    public String getTargetNodeId() {
        return targetNodeId;
    }

    public void setTargetNodeId(String targetNodeId) {
        this.targetNodeId = targetNodeId;
    }

    public int getTargetNodeCount() {
        return targetNodeCount;
    }

    public void setTargetNodeCount(int targetNodeCount) {
        this.targetNodeCount = targetNodeCount;
    }

    @Override
    public String getDeprecatedStartParameter() {
        return null;
    }

    public IParameterService getParameterService() {
        return parameterService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
