package org.jumpmind.symmetric.io.data.writer;

public class ResolvedData {

    private long rowNumber;

    private String resolvedData;
    
    private boolean ignoreRow;

    public ResolvedData(long rowNumber, String resolvedData, boolean ignoreRow) {
        this.rowNumber = rowNumber;
        this.resolvedData = resolvedData;
        this.ignoreRow = ignoreRow;
    }
    
    public boolean isIgnoreRow() {
        return ignoreRow;
    }

    public String getResolvedData() {
        return resolvedData;
    }

    public long getRowNumber() {
        return rowNumber;
    }
}
