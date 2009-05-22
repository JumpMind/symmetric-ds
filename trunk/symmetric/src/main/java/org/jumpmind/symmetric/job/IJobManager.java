package org.jumpmind.symmetric.job;

import java.util.Timer;

public interface IJobManager {

    public void startJobs();
    
    public void stopJobs();
    
    public void addTimer(String timerName, Timer timer);
    
}
