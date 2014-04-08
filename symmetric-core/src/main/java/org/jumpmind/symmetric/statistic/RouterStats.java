package org.jumpmind.symmetric.statistic;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.model.DataGap;

public class RouterStats {

    private long startDataId;
    
    private long endDataId;

    private long dataReadCount;
        
    private long peekAheadFillCount;

    private List<DataGap> dataGaps;
    
    private Set<String> transactions;
    
    public RouterStats() {
    }
    
    public RouterStats(long startDataId, long endDataId, long dataReadCount, long peekAheadFillCount, 
            List<DataGap> dataGaps, Set<String> transactions) {
        this.startDataId = startDataId;
        this.endDataId = endDataId;
        this.dataReadCount = dataReadCount;
        this.peekAheadFillCount = peekAheadFillCount;
        this.dataGaps = dataGaps;
        this.transactions = transactions;
    }
    
    @Override
    public String toString() {
        return "{ startDataId: " + startDataId + ", endDataId: " + endDataId + ", dataReadCount: " + dataReadCount +
                ", peekAheadFillCount: " + peekAheadFillCount + ", dataGaps: " + dataGaps.toString() + 
                ", transactions: " + transactions.toString() + " }";
    }

    public long getStartDataId() {
        return startDataId;
    }

    public void setStartDataId(long startDataId) {
        this.startDataId = startDataId;
    }

    public long getEndDataId() {
        return endDataId;
    }

    public void setEndDataId(long endDataId) {
        this.endDataId = endDataId;
    }

    public long getDataReadCount() {
        return dataReadCount;
    }

    public void setDataReadCount(long dataReadCount) {
        this.dataReadCount = dataReadCount;
    }

    public long getPeekAheadFillCount() {
        return peekAheadFillCount;
    }

    public void setPeekAheadFillCount(long peekAheadFillCount) {
        this.peekAheadFillCount = peekAheadFillCount;
    }

    public List<DataGap> getDataGaps() {
        return dataGaps;
    }

    public void setDataGaps(List<DataGap> dataGaps) {
        this.dataGaps = dataGaps;
    }

    public Set<String> getTransactions() {
        return transactions;
    }

    public void setTransactions(Set<String> transactions) {
        this.transactions = transactions;
    }
}
