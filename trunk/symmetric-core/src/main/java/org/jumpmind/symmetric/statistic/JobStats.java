package org.jumpmind.symmetric.statistic;

import java.util.Date;

public class JobStats extends AbstractNodeHostStats {

    private String jobName;
    private long processedCount;

    public JobStats() {
    }

    public JobStats(String nodeId, String hostName, Date startTime, Date endTime, String jobName) {
        super(nodeId, hostName, startTime, endTime);
        this.jobName = jobName;
    }

    public JobStats(String nodeId, String hostName, Date startTime, Date endTime, String jobName,
            long processedCount) {
        this(nodeId, hostName, startTime, endTime, jobName);
        this.processedCount = processedCount;
    }
    
    public JobStats(String jobName, long startTime, long endTime, long processedCount) {
        this(null, null, new Date(startTime), new Date(endTime), jobName);
        this.processedCount = processedCount;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String channelId) {
        this.jobName = channelId;
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(long processedCount) {
        this.processedCount = processedCount;
    }

}