package org.jumpmind.symmetric.statistic;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;

public class MockStatisticManager implements IStatisticManager {

    public List<ProcessInfo> getProcessInfos() {
        return null;
    }
    
    public void removeProcessInfo(ProcessInfoKey key) {
    }
    
    public ProcessInfo newProcessInfo(ProcessInfoKey key) {
        return null;
    }
    
    public Set<String> getNodesWithProcessesInError() {
        return null;
    }
    
    public void flush() {
    }

    public void addJobStats(String jobName, long startTime, long endTime, long processedCount) {
    }
    
    public void incrementRestart() {
    }

    public void incrementNodesPulled(long count) {
    }

    public void incrementNodesRegistered(long count) {
    }

    public void incrementNodesPushed(long count) {
    }

    public void incrementTotalNodesPulledTime(long count) {
    }

    public void incrementTotalNodesPushedTime(long count) {
    }

    public void incrementNodesRejected(long count) {
    }

    public synchronized void incrementDataLoadedErrors(String channelId, long count) {
    }

    public synchronized void incrementDataBytesLoaded(String channelId, long count) {
    }

    public synchronized void incrementDataBytesSent(String channelId, long count) {
    }

    public synchronized void incrementDataEventInserted(String channelId, long count) {
    }

    public synchronized void incrementDataExtractedErrors(String channelId, long count) {
    }

    public synchronized void incrementDataBytesExtracted(String channelId, long count) {
    }

    public synchronized void setDataUnRouted(String channelId, long count) {
    }

    public synchronized void incrementDataRouted(String channelId, long count) {
    }

    public synchronized void incrementDataSentErrors(String channelId, long count) {
    }

    public void incrementDataExtracted(String channelId, long count) {
    }

    public void incrementDataLoaded(String channelId, long count) {
    }

    public void incrementDataSent(String channelId, long count) {
    };

    public Map<String, ChannelStats> getWorkingChannelStats() {
        return null;
    }
    
    public HostStats getWorkingHostStats() {
        return null;
    }

    public void incrementNodesLoaded(long count) {

    }

    public void incrementNodesDisabled(long count) {

    }

    public void incrementPurgedBatchIncomingRows(long count) {

    }

    public void incrementPurgedBatchOutgoingRows(long count) {

    }

    public void incrementPurgedDataRows(long count) {

    }

    public void incrementPurgedDataEventRows(long count) {

    }

    public void incrementTriggersRemovedCount(long count) {

    }

    public void incrementTriggersRebuiltCount(long count) {

    }

    public void incrementTriggersCreatedCount(long count) {

    }
}