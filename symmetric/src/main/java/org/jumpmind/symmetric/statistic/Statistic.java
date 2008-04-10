package org.jumpmind.symmetric.statistic;

import java.math.BigDecimal;
import java.util.Date;

public class Statistic {

    private StatisticName name;

    private String nodeId;

    private Date captureStartTimeMs;

    private BigDecimal total;
    
    private long count;

    public Statistic(StatisticName name, String nodeId) {
        this(name, nodeId, new Date());
    }
    
    public Statistic(StatisticName name, String nodeId, Date startTime) {
        this.name = name;
        this.nodeId = nodeId;
        this.captureStartTimeMs = startTime;
        this.total = BigDecimal.ZERO;
    }    

    public StatisticName getName() {
        return name;
    }

    public String getNodeId() {
        return nodeId;
    }

    public BigDecimal getTotal() {
        return total;
    }
    
    public BigDecimal getAverageValue() {
        if (total != null && count > 0) {
            return total.divide(new BigDecimal(count));
        } else {
            return BigDecimal.ZERO;
        }
    }
    
    public void add(int v) {
        add(v,1);
    }

    public void add(BigDecimal v) {
        add(v,1);
    }
    
    public void add(long v) {
        add(v,1);
    }

    public void add(long v, int count) {
        this.total = this.total.add(new BigDecimal(v));
        this.count += this.count;
    }

    public void add(int v, int count) {
        this.total = this.total.add(new BigDecimal(v));
        this.count += this.count;
    }

    public void add(BigDecimal v, int count) {
        this.total = this.total.add(v);
        this.count += this.count;
    }

    public Date getCaptureStartTimeMs() {
        return captureStartTimeMs;
    }

    public long getCount() {
        return count;
    }

}
