package org.jumpmind.symmetric.model;

import java.util.Date;

public class Sequence {

    private String sequenceName;
    private long currentValue;
    private int incrementBy;
    private long minValue;
    private long maxValue;
    private Date createTime;
    private String lastUpdateBy;
    private Date lastUpdateTime;
    private boolean cycle;
    
    public Sequence() {
    }

    public Sequence(String sequenceName, long currentValue, int incrementBy, long minValue,
            long maxValue, String lastUpdateBy, boolean cycle) {
        this.sequenceName = sequenceName;
        this.currentValue = currentValue;
        this.incrementBy = incrementBy;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.lastUpdateBy = lastUpdateBy;
        this.cycle = cycle;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public long getCurrentValue() {
        return currentValue;
    }

    public void setCurrentValue(long value) {
        this.currentValue = value;
    }

    public int getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(int incrementBy) {
        this.incrementBy = incrementBy;
    }

    public long getMinValue() {
        return minValue;
    }

    public void setMinValue(long minValue) {
        this.minValue = minValue;
    }

    public long getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(long maxValue) {
        this.maxValue = maxValue;
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

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }
    
    public boolean isCycle() {
        return cycle;
    }
}
