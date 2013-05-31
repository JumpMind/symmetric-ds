package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;

/**
 * Definition of a channel and it's priority. A channel is a group of tables
 * that get synchronized together.
 */
public class Channel implements Serializable {

    private static final long serialVersionUID = -8183376200537307264L;

    private String channelId;

    private int processingOrder;

    private int maxBatchSize;

    private int maxBatchToSend;
    
    private int maxDataToRoute = 10000;

    private boolean enabled;
    
    private boolean useOldDataToRoute = true;
    
    private boolean useRowDataToRoute = true;
    
    private boolean usePkDataToRoute = true;
    
    private boolean containsBigLob = false;

    private String batchAlgorithm = "default";

    private long extractPeriodMillis = 0;
    
    private String dataLoaderType = "default";
    
    private Date createTime;
    
    private Date lastUpdateTime;
    
    private String lastUpdateBy;

    public Channel() {
    }

    public Channel(String id, int processingOrder) {
        this.channelId = id;
        this.processingOrder = processingOrder;
    }

    public Channel(String id, int processingOrder, int maxBatchSize, int maxBatchToSend, boolean enabled,
            long extractPeriodMillis, boolean containsBigLobs) {
        this(id, processingOrder);
        this.maxBatchSize = maxBatchSize;
        this.maxBatchToSend = maxBatchToSend;
        this.enabled = enabled;
        this.extractPeriodMillis = extractPeriodMillis;
        this.containsBigLob = containsBigLobs;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String id) {
        this.channelId = id;
    }

    public int getProcessingOrder() {
        return processingOrder;
    }

    public void setProcessingOrder(int priority) {
        this.processingOrder = priority;
    }
    
    public void setMaxDataToRoute(int maxDataToRoute) {
        this.maxDataToRoute = maxDataToRoute;
    }
    
    public int getMaxDataToRoute() {
        return maxDataToRoute;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxNumberOfEvents) {
        this.maxBatchSize = maxNumberOfEvents;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxBatchToSend() {
        return maxBatchToSend;
    }

    public void setMaxBatchToSend(int maxBatchToSend) {
        this.maxBatchToSend = maxBatchToSend;
    }

    /**
     * Check to see if this channel id matches one of the channels in the
     * collection
     * 
     * @return true if a match is found
     */
    public boolean isInList(Collection<? extends NodeChannel> channels) {
        if (channels != null) {
            for (NodeChannel channel : channels) {
                if (channel.getChannelId().equals(channelId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void setBatchAlgorithm(String batchAlgorithm) {
        this.batchAlgorithm = batchAlgorithm;
    }

    public String getBatchAlgorithm() {
        return batchAlgorithm;
    }

    public long getExtractPeriodMillis() {
        return extractPeriodMillis;
    }

    public void setExtractPeriodMillis(long extractPeriodMillis) {
        this.extractPeriodMillis = extractPeriodMillis;
    }

    public void setUseOldDataToRoute(boolean useOldDataToRoute) {
        this.useOldDataToRoute = useOldDataToRoute;
    }
 
    public boolean isUseOldDataToRoute() {
        return useOldDataToRoute;
    }
    
    public void setUseRowDataToRoute(boolean useRowDataToRoute) {
        this.useRowDataToRoute = useRowDataToRoute;
    }
    
    public boolean isUseRowDataToRoute() {
        return useRowDataToRoute;
    }
    
    public void setUsePkDataToRoute(boolean usePkDataToRoute) {
        this.usePkDataToRoute = usePkDataToRoute;
    }
    
    public boolean isUsePkDataToRoute() {
        return usePkDataToRoute;
    }
    
    public void setContainsBigLob(boolean containsBigLobs) {
        this.containsBigLob = containsBigLobs;
    }
    
    public boolean isContainsBigLob() {
        return containsBigLob;
    }
    
    public void setDataLoaderType(String dataloader) {
        this.dataLoaderType = dataloader;
    }
    
    public String getDataLoaderType() {
        return dataLoaderType;
    }
    
    public Date getCreateTime() {
        return createTime;
    }
    
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public String getLastUpdateBy() {
        return lastUpdateBy;
    }
    
    public void setLastUpdateBy(String lastUpdateBy) {
        this.lastUpdateBy = lastUpdateBy;
    }
    
    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(Date lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    @Override
    public int hashCode() {
        if (channelId != null) {
            return channelId.hashCode();
        } else {
            return super.hashCode();
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (channelId != null) {
            if (obj instanceof Channel) {
                return channelId.equals(((Channel) obj).channelId);
            } else {
                return false;
            }
        } else {
            return super.equals(obj);
        }
    }

    @Override
    public String toString() {
        if (channelId != null) {
            return channelId;
        } else {
            return super.toString();
        }        
    }
}