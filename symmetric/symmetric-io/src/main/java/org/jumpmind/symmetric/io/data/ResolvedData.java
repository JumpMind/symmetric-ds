package org.jumpmind.symmetric.io.data;

public class ResolvedData {

    private long rowNumber;

    private String resolvedData;

    public ResolvedData(long rowNumber, String resolvedData) {
        this.rowNumber = rowNumber;
        this.resolvedData = resolvedData;
    }

    public String getResolvedData() {
        return resolvedData;
    }

    public long getRowNumber() {
        return rowNumber;
    }
}
