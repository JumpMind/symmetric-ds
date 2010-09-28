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

import javax.sql.DataSource;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedMetric;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for a job")
/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
abstract public class AbstractJob implements Runnable, BeanNameAware {

    DataSource dataSource;

    protected final ILog log = LogFactory.getLog(getClass());

    protected IParameterService parameterService;

    private String beanName;

    private boolean requiresRegistration = true;

    private IRegistrationService registrationService;

    private boolean paused = false;

    private Date lastFinishTime;

    private boolean running = false;

    private long lastExecutionTimeInMs;

    private long totalExecutionTimeInMs;

    private long numberOfRuns;

    private boolean started;
    
    private long timeBetweenRunsInMs;
    
    private String cronExpression;
    
    private boolean hasNotRegisteredMessageBeenLogged = false;

    public String getName() {
        return beanName;
    }

    @ManagedOperation(description = "Run this job is it isn't already running")
    public boolean invoke() {
        return invoke(true);
    }
    
    public boolean invoke(boolean force) {
        boolean ran = false;
        try {
            ISymmetricEngine engine = StandaloneSymmetricEngine.findEngineByName(parameterService
                    .getString(ParameterConstants.ENGINE_NAME));

            if (engine == null) {
                log.info("SymmetricEngineMissing", beanName);
            } else if (engine.isStarted()) {
                if (!paused || force) {
                    if (!running) {
                        running = true;
                        synchronized (this) {
                            ran = true;
                            long ts = System.currentTimeMillis();
                            try {
                                if (!requiresRegistration
                                        || (requiresRegistration && registrationService
                                                .isRegisteredWithServer())) {
                                    hasNotRegisteredMessageBeenLogged = false;
                                    doJob();
                                } else {
                                    if (!hasNotRegisteredMessageBeenLogged) {
                                        log.warn("SymmetricEngineNotRegistered", getName());
                                        hasNotRegisteredMessageBeenLogged = true;
                                    }
                                }
                            } finally {
                                lastFinishTime = new Date();
                                lastExecutionTimeInMs = System.currentTimeMillis() - ts;
                                totalExecutionTimeInMs += lastExecutionTimeInMs;
                                numberOfRuns++;
                                running = false;
                            }
                        }
                    }
                }
            } else {
                log.info("SymmetricEngineNotStarted");
            }
        } catch (final Throwable ex) {
            log.error(ex);
        }        
        
        return ran;
    }
    
    
    /**
     * This method is called from the job
     */
    public void run() {
        invoke(false);
    }

    abstract void doJob() throws Exception;

    public void setBeanName(final String beanName) {
        this.beanName = beanName;
    }

    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setRequiresRegistration(boolean requiresRegistration) {
        this.requiresRegistration = requiresRegistration;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }
    
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

    public void setStarted(boolean started) {
        this.started = started;
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
    
    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }
    
    @ManagedAttribute(description = "If set, this is the cron expression that governs when the job will run")
    public String getCronExpression() {
        return cronExpression;
    }
    
    public void setTimeBetweenRunsInMs(long timeBetweenRunsInMs) {
        this.timeBetweenRunsInMs = timeBetweenRunsInMs;
    }
    
    @ManagedAttribute(description = "If the cron expression isn't set.  This is the amount of time that will pass before the periodic job runs again.")
    public long getTimeBetweenRunsInMs() {
        return timeBetweenRunsInMs;
    }

}