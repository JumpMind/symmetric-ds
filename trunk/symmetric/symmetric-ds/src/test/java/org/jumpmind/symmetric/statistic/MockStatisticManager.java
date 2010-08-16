package org.jumpmind.symmetric.statistic;

public class MockStatisticManager implements IStatisticManager {

    public void flush() {
    }

    public synchronized void incrementDataLoadedErrors(String channelId, long count) {
    }

    public synchronized void incrementDataBytesLoaded(String channelId, long count) {
    }

    public synchronized void incrementDataBytesTransmitted(String channelId, long count) {
    }

    public synchronized void incrementDataEventInserted(String channelId, long count) {
    }

    public synchronized void incrementDataExtractedErrors(String channelId, long count) {
    }

    public synchronized void incrementDataBytesExtracted(String channelId, long count) {
    }

    public synchronized void incrementDataUnRouted(String channelId, long count) {
    }

    public synchronized void incrementDataRouted(String channelId, long count) {
    }
    
    public synchronized void incrementDataTransmittedErrors(String channelId, long count) {
        
    }

}
