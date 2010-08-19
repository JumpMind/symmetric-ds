package org.jumpmind.symmetric.statistic;

public class MockStatisticManager implements IStatisticManager {

    public void flush() {
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
    
    public void incrementDataSent(String channelId, long count) {};

}
