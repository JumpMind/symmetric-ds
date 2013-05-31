package org.jumpmind.symmetric.job;

import java.util.List;



/*
 * An API that provides access to individual jobs and provides job
 * life cycle control
 */
public interface IJobManager {

    public void startJobs();
    
    public void stopJobs();
    
    public void destroy();
    
    public List<IJob> getJobs();
    
    public IJob getJob(String name);
    
}