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

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.JobDefinition;
import org.jumpmind.symmetric.model.JobDefinition.StartupType;
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
    
    public JobManager(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        
        this.engine = engine;

        ISymmetricDialect dialect = engine.getSymmetricDialect();
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
        jobDefitions = builtInJobs.syncBuiltInJobs(jobDefitions, engine, taskScheduler); // TODO save built in hobs
        
        this.jobs = new ArrayList<IJob>();
        
        for (JobDefinition jobDefinition : jobDefitions) {
            jobs.add(jobCreator.createJob(jobDefinition, engine, taskScheduler));
        }
    }

    protected List<JobDefinition> loadJobs(ISymmetricEngine engine) {
        // List<IJob>  jobs = new ArrayList<IJob>();
       return sqlTemplate.query(getSql("loadCustomJobs"), new JobMapper());
      //  return new ArrayList<JobDefinition>();
//        return null;
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
     * Start the jobs if they are configured to be started in
     * symmetric.properties
     */
    @Override
    public synchronized void startJobs() {
        for (IJob job : jobs) {
            if (isAutoStartConfigured(job) 
                    && StartupType.AUTOMATIC == job.getJobDefinition().getStartupType()) {
                job.start();
            } else {
                log.info("Job {} not configured for auto start", job.getName());
            }
        }
    }
    

    @Override
    public synchronized void startJobsAfterConfigChange() {
        for (IJob job : jobs) {
            if (isAutoStartConfigured(job) 
                    && StartupType.AUTOMATIC == job.getJobDefinition().getStartupType() 
                    && !job.isStarted()) {
                job.start();
            }
        }        
    }
    
    protected boolean isAutoStartConfigured(IJob job) {
        String key = "start." + job.getName();
        return engine.getParameterService().is(key, true);
    }

    @Override
    public synchronized void stopJobs() {
        for (IJob job : jobs) {
            job.stop();
        }      
        Thread.interrupted();
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
        Object[] args = { job.getDescription(), job.getJobType().toString(), job.getSchedule(), 
                job.getStartupType().toString(), job.getScheduleType().toString(), job.getJobExpression(), 
                job.getCreateBy(), job.getLastUpdateBy(), job.getJobName() };

        if (sqlTemplate.update(getSql("updateJobSql"), args) == 0) {
            sqlTemplate.update(getSql("insertJobSql"), args);
        } 
        init();
        startJobsAfterConfigChange();
    }
}
