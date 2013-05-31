package org.jumpmind.symmetric.service;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jumpmind.symmetric.statistic.ChannelStats;
import org.jumpmind.symmetric.statistic.HostStats;
import org.jumpmind.symmetric.statistic.JobStats;


/**
 * This service provides an API to access captured statistics
 */
public interface IStatisticService {

    public void save(ChannelStats stats);
    
    public void save(HostStats stats);
    
    public void save(JobStats stats);
    
    public TreeMap<Date, Map<String, ChannelStats>> getChannelStatsForPeriod(Date start, Date end, String nodeId, int periodSizeInMinutes);
    
    public TreeMap<Date, HostStats> getHostStatsForPeriod(Date start, Date end, String nodeId, int periodSizeInMinutes);

    public List<JobStats> getJobStatsForPeriod(Date start, Date end, String nodeId);
    

}