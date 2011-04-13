package org.jumpmind.symmetric.data.model;

public class Batch {

    protected String sourceNodeId;
    protected boolean initialLoad;
    protected long batchId;
    protected String channelId;

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public long getBatchId() {
        return batchId;
    }

    public String getChannelId() {
        return channelId;
    }

    public boolean isInitialLoad() {
        return initialLoad;
    }
}
