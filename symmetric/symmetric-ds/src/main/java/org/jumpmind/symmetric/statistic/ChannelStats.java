package org.jumpmind.symmetric.statistic;

import java.util.Date;

public class ChannelStats extends AbstractStats {

    private String channelId;
    private long dataRouted;
    private long dataUnRouted;
    private long dataBytesExtracted;
    private long dataExtractedErrors;
    private long dataEventInserted;
    private long dataBytesTransmitted;
    private long dataTransmittedErrors;
    private long dataBytesLoaded;
    private long dataLoadedErrors;
    
    public ChannelStats(String nodeId, String hostName, Date startTime, Date endTime,
            String channelId) {
        super(nodeId, hostName, startTime, endTime);
        this.channelId = channelId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public long getDataRouted() {
        return dataRouted;
    }

    public void setDataRouted(long dataRouted) {
        this.dataRouted = dataRouted;
    }
    
    public void incrementDataRouted(long count) {
        this.dataRouted += count;
    }

    public long getDataUnRouted() {
        return dataUnRouted;
    }

    public void setDataUnRouted(long dataUnRouted) {
        this.dataUnRouted = dataUnRouted;
    }
    
    public void incrementDataUnRouted(long count) {
        this.dataUnRouted += count;
    }

    public long getDataBytesExtracted() {
        return dataBytesExtracted;
    }

    public void setDataBytesExtracted(long dataExtracted) {
        this.dataBytesExtracted = dataExtracted;
    }
    
    public void incrementDataBytesExtracted(long count) {
        this.dataBytesExtracted += count;
    }

    public long getDataExtractedErrors() {
        return dataExtractedErrors;
    }

    public void setDataExtractedErrors(long dataExtractedErrors) {
        this.dataExtractedErrors = dataExtractedErrors;
    }
    
    public void incrementDataExtractedErrors(long count) {
        this.dataExtractedErrors += count;
    }

    public long getDataEventInserted() {
        return dataEventInserted;
    }

    public void setDataEventInserted(long dataEventInserted) {
        this.dataEventInserted = dataEventInserted;
    }
    
    public void incrementDataEventInserted(long count) {
        this.dataEventInserted += count;
    }

    public long getDataBytesTransmitted() {
        return dataBytesTransmitted;
    }

    public void setDataBytesTransmitted(long dataTransmitted) {
        this.dataBytesTransmitted = dataTransmitted;
    }
    
    public void incrementDataBytesTransmitted(long count) {
        this.dataBytesTransmitted += count;
    }

    public void setDataTransmittedErrors(long dataTransmittedErrors) {
        this.dataTransmittedErrors = dataTransmittedErrors;
    }
    
    public long getDataTransmittedErrors() {
        return dataTransmittedErrors;
    }
    
    public void incrementDataTransmittedErrors(long count) {
        this.dataTransmittedErrors += count;    
    }
    
    public long getDataBytesLoaded() {
        return dataBytesLoaded;
    }

    public void setDataBytesLoaded(long dataLoaded) {
        this.dataBytesLoaded = dataLoaded;
    }
    
    public void incrementDataBytesLoaded(long count) {
        this.dataBytesLoaded += count;
    }

    public long getDataLoadedErrors() {
        return dataLoadedErrors;
    }

    public void setDataLoadedErrors(long dataLoadedErrors) {
        this.dataLoadedErrors = dataLoadedErrors;
    }
    
    public void incrementDataLoadedErrors(long count) {
        this.dataLoadedErrors += count;
    }

}
