package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

/**
 * A composite parent for {@link Channel} and {@link NodeChannelControl}
 */
public class NodeChannel implements Serializable {

    private static final long serialVersionUID = 1L;

    private Channel channel;
    
    private NodeChannelControl nodeChannelControl;

    public NodeChannel() {
        channel = new Channel();
        nodeChannelControl = new NodeChannelControl();
    }

    public NodeChannel(String channelId) {
        channel = new Channel();
        nodeChannelControl = new NodeChannelControl();
        nodeChannelControl.setChannelId(channelId);
        channel.setChannelId(channelId);
    }

    public String getChannelId() {
        return channel.getChannelId();
    }

    public int getMaxBatchSize() {
        return channel.getMaxBatchSize();
    }

    public void setMaxBatchSize(int maxBatchSize) {
        channel.setMaxBatchSize(maxBatchSize);
    }

    public int getMaxBatchToSend() {
        return channel.getMaxBatchToSend();
    }

    public void setMaxBatchToSend(int maxBatchToSend) {
        channel.setMaxBatchToSend(maxBatchToSend);
    }

    public void setMaxDataToRoute(int maxDataToRoute) {
        channel.setMaxDataToRoute(maxDataToRoute);
    }

    public int getMaxDataToRoute() {
        return channel.getMaxDataToRoute();
    }

    public void setUseOldDataToRoute(boolean useOldDataToRoute) {
        channel.setUseOldDataToRoute(useOldDataToRoute);
    }

    public boolean isUseOldDataToRoute() {
        return channel.isUseOldDataToRoute();
    }

    public void setUseRowDataToRoute(boolean useRowDataToRoute) {
        channel.setUseRowDataToRoute(useRowDataToRoute);
    }

    public boolean isUseRowDataToRoute() {
        return channel.isUseRowDataToRoute();
    }

    public void setUsePkDataToRoute(boolean usePkDataToRoute) {
        channel.setUsePkDataToRoute(usePkDataToRoute);
    }

    public boolean isUsePkDataToRoute() {
        return channel.isUsePkDataToRoute();
    }

    public int getProcessingOrder() {
        return channel.getProcessingOrder();
    }

    public String getBatchAlgorithm() {
        return channel.getBatchAlgorithm();
    }

    public void setBatchAlgorithm(String batchAlgorithm) {
        channel.setBatchAlgorithm(batchAlgorithm);
    }

    public void setEnabled(boolean enabled) {
        channel.setEnabled(enabled);
    }

    public boolean isEnabled() {
        return channel.isEnabled();
    }

    public boolean isSuspendEnabled() {
        return nodeChannelControl.isSuspendEnabled();
    }

    public boolean isIgnoreEnabled() {
        return nodeChannelControl.isIgnoreEnabled();
    }

    public String getNodeId() {
        return nodeChannelControl.getNodeId();
    }

    public void setNodeId(String nodeId) {
        nodeChannelControl.setNodeId(nodeId);
    }

    public void setLastExtractTime(Date lastExtractedTime) {
        nodeChannelControl.setLastExtractTime(lastExtractedTime);
    }

    public Date getLastExtractTime() {
        return nodeChannelControl.getLastExtractTime();
    }

    public void setIgnoreEnabled(boolean ignored) {
        nodeChannelControl.setIgnoreEnabled(ignored);
    }

    public void setProcessingOrder(int priority) {
        channel.setProcessingOrder(priority);
    }

    public void setChannelId(String id) {
        channel.setChannelId(id);
        nodeChannelControl.setChannelId(id);
    }
    
    public void setLastUpdateTime(Date date) {
        channel.setLastUpdateTime(date);
    }
    
    public void setCreateTime(Date date) {
        channel.setCreateTime(date);
    }
    
    public void setLastUpdateBy(String lastUpdateBy) {
        channel.setLastUpdateBy(lastUpdateBy);
    }

    public void setSuspendEnabled(boolean suspended) {
        nodeChannelControl.setSuspendEnabled(suspended);
    }

    public Channel getChannel() {
        return channel;
    }

    public NodeChannelControl getNodeChannelControl() {
        return nodeChannelControl;
    }

    public long getExtractPeriodMillis() {
        return channel.getExtractPeriodMillis();
    }

    public void setExtractPeriodMillis(long extractPeriodMillis) {
        channel.setExtractPeriodMillis(extractPeriodMillis);
    }

    public void setContainsBigLobs(boolean containsBigLobs) {
        this.channel.setContainsBigLob(containsBigLobs);
    }

    public boolean isContainsBigLob() {
        return this.channel.isContainsBigLob();
    }
    
    public void setDataLoaderType(String type) {
        channel.setDataLoaderType(type);
    }
    
    public String getDataLoaderType() {
        return channel.getDataLoaderType();
    }

}