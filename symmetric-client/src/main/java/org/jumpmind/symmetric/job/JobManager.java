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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.model.JobDefinition;
import org.jumpmind.symmetric.service.impl.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * @see IJobManager
 */
public class JobManager extends AbstractService implements IJobManager {

    static final Logger log = LoggerFactory.getLogger(JobManager.class);

    private List<IJob> jobs;
    private ThreadPoolTaskScheduler taskScheduler;
    private ISymmetricEngine engine;
    private JobCreator jobCreator = new JobCreator();
    
    private boolean started = false;
    
    public JobManager(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        
        this.engine = engine;

        this.taskScheduler = new ThreadPoolTaskScheduler();
        setSqlMap(new JobManagerSqlMap(engine.getSymmetricDialect().getPlatform(), createSqlReplacementTokens()));        
                
        this.taskScheduler.setThreadNamePrefix(String.format("%s-job-", engine.getParameterService().getEngineName()));
        this.taskScheduler.setPoolSize(20);
        this.taskScheduler.initialize();    
    }
    
    protected Map<String, String> createSqlReplacementTokens() {
        Map<String, String> replacementTokens = createSqlReplacementTokens(this.tablePrefix, symmetricDialect.getPlatform()
                .getDatabaseInfo().getDelimiterToken());
        replacementTokens.putAll(symmetricDialect.getSqlReplacementTokens());
        return replacementTokens;
    }    
    
    @Override
    public void init() {
        if (this.jobs != null && !this.jobs.isEmpty()) {
            for (IJob job : jobs) {
                if (job.isStarted()) {                    
                    job.stop();
                }
            }
        }
        List<JobDefinition> jobDefitions = loadJobs(engine);
        
        BuiltInJobs builtInJobs = new BuiltInJobs();
        jobDefitions = builtInJobs.syncBuiltInJobs(jobDefitions, engine, taskScheduler); // TODO save built in jobs
        
        this.jobs = new ArrayList<IJob>();
        
        for (JobDefinition jobDefinition : jobDefitions) {
            IJob job = jobCreator.createJob(jobDefinition, engine, taskScheduler);
            if (job != null) {                
                jobs.add(job);
            }
        }
    }

    protected List<JobDefinition> loadJobs(ISymmetricEngine engine) {
       return sqlTemplate.query(getSql("loadCustomJobs"), new JobMapper());
    }
    
    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public IJob getJob(String name) {
        for (IJob job : jobs) {
            if (job.getName().equals(name)) {
                return job;
            }
        }
        return null;
    }
    
    /*
     * Start the jobs if they are configured to be started
     */
    @Override
    public synchronized void startJobs() {
        for (IJob job : jobs) {
            if (isAutoStartConfigured(job)) {
                job.start();
            } else {
                log.info("Job {} not configured for auto start", job.getName());
            }
        }
        started = true;
    }
    

    @Override
    public synchronized void startJobsAfterConfigChange() {
        for (IJob job : jobs) {
            if (isAutoStartConfigured(job) 
                    && !job.isStarted()) {
                job.start();
            }
        }        
    }
    
    protected boolean isAutoStartConfigured(IJob job) {
        String startParameter = job.getJobDefinition().getStartParameter();
        String autoStartValue = engine.getParameterService().getString(startParameter);
        if (StringUtils.isEmpty(autoStartValue) && job.getDeprecatedStartParameter() != null) {
            startParameter = job.getDeprecatedStartParameter();
        }
        
        return engine.getParameterService().is(startParameter, true);
    }

    @Override
    public synchronized void stopJobs() {
        for (IJob job : jobs) {
            job.stop();
        }      
        Thread.interrupted();
        started = false;
    }
    
    @Override
    public synchronized void destroy() {
        stopJobs();
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    @Override
    public List<IJob> getJobs() {
        return jobs;
    }

    @Override
    public void saveJob(JobDefinition job) {
        Object[] args = { job.getDescription(), job.getJobType().toString(),  
                job.getJobExpression(), job.getCreateBy(), job.getLastUpdateBy(), job.getJobName() };

        if (sqlTemplate.update(getSql("updateJobSql"), args) == 0) {
            sqlTemplate.update(getSql("insertJobSql"), args);
        } 
        init();
        startJobsAfterConfigChange();
    }
    
    @Override
    public void removeJob(String name) {
        Object[] args = { name };

        if (sqlTemplate.update(getSql("deleteJobSql"), args) == 0) {
            throw new SymmetricException("Failed to remove job " + name + ".  Note that BUILT_IN jobs cannot be removed.");
        } 
        init();
        startJobsAfterConfigChange();        
    }
}
