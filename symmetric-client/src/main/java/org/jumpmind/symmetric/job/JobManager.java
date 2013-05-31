package org.jumpmind.symmetric.job;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * @see IJobManager
 */
public class JobManager implements IJobManager {

    static final Logger log = LoggerFactory.getLogger(JobManager.class);

    private List<IJob> jobs;
    
    private ThreadPoolTaskScheduler taskScheduler;
    
    public JobManager(ISymmetricEngine engine) {
        
        this.taskScheduler = new ThreadPoolTaskScheduler();
        this.taskScheduler.setThreadNamePrefix(String.format("%s-job-", engine.getParameterService().getEngineName()));
        this.taskScheduler.setPoolSize(20);
        this.taskScheduler.initialize();
        
        this.jobs = new ArrayList<IJob>();
        this.jobs.add(new RouterJob(engine, taskScheduler));
        this.jobs.add(new PushJob(engine, taskScheduler));
        this.jobs.add(new PullJob(engine, taskScheduler));
        this.jobs.add(new OutgoingPurgeJob(engine, taskScheduler));
        this.jobs.add(new IncomingPurgeJob(engine, taskScheduler));
        this.jobs.add(new DataGapPurgeJob(engine, taskScheduler));
        this.jobs.add(new StatisticFlushJob(engine, taskScheduler));
        this.jobs.add(new SyncTriggersJob(engine, taskScheduler));
        this.jobs.add(new HeartbeatJob(engine, taskScheduler));
        this.jobs.add(new WatchdogJob(engine, taskScheduler));
        this.jobs.add(new StageManagementJob(engine, taskScheduler, engine.getStagingManager()));
        this.jobs.add(new RefreshCacheJob(engine, taskScheduler));
        this.jobs.add(new FileSyncTrackerJob(engine,taskScheduler));
        this.jobs.add(new FileSyncPullJob(engine,taskScheduler));
        this.jobs.add(new FileSyncPushJob(engine,taskScheduler));
        
    }

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
    public synchronized void startJobs() {
        for (IJob job : jobs) {
            if (job.isAutoStartConfigured()) {
                job.start();
            } else {
                log.info("Job {} not configured for auto start", job.getName());
            }
        }
    }

    public synchronized void stopJobs() {
        for (IJob job : jobs) {
            job.stop();
        }      
        Thread.interrupted();
    }
    
    public synchronized void destroy () {
        stopJobs();
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    public List<IJob> getJobs() {
        return jobs;
    }
}
