package org.jumpmind.symmetric.job;

import java.util.Date;

public interface IJob {

    public void start();

    public boolean stop();

    public String getName();
    
    public String getClusterLockName();
    
    public boolean isClusterable();

    public void pause();

    public void unpause();

    public boolean isPaused();

    public boolean isStarted();
    
    public boolean isAutoStartConfigured();

    public long getLastExecutionTimeInMs();

    public Date getLastFinishTime();

    public boolean isRunning();

    public long getNumberOfRuns();

    public long getTotalExecutionTimeInMs();

    public long getAverageExecutionTimeInMs();

    public String getCronExpression();

    public long getTimeBetweenRunsInMs();  
    
    public boolean invoke(boolean force);

}