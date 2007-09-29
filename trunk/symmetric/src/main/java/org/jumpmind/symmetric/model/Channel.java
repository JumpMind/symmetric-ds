package org.jumpmind.symmetric.model;

/**
 * Definition of a channel and it's priority. A channel is a group of tables
 * that get synchronized together.
 * 
 * @author chenson
 */
public class Channel extends BaseEntity {

    private static final long serialVersionUID = -8183376200537307264L;

    private String id;

    private int processingOrder;

    private int maxBatchSize;

    private String description;
    
    private boolean enabled;

    public Channel() {
    }

    public Channel(String id, int priority) {
        this.id = id;
        this.processingOrder = priority;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getProcessingOrder() {
        return processingOrder;
    }

    public void setProcessingOrder(int priority) {
        this.processingOrder = priority;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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
}
