package org.jumpmind.symmetric.extract;

import org.jumpmind.symmetric.model.TriggerHistory;

public class SelectFromTableOptions {
    protected TriggerHistory triggerHistory;
    protected String initialLoadSql;
    protected int expectedCommaCount;
    protected boolean selectedAsCsv;
    protected boolean objectValuesWillNeedEscaped;
    protected boolean[] columnPositionUsingTemplate;
    protected boolean checkRowLength;
    protected long rowMaxLength;
    protected boolean returnLobObjects;
    protected int maxBatchSize;

    public SelectFromTableOptions() {
    }

    public TriggerHistory getTriggerHistory() {
        return triggerHistory;
    }

    public SelectFromTableOptions triggerHistory(TriggerHistory triggerHistory) {
        this.triggerHistory = triggerHistory;
        return this;
    }

    public String getInitialLoadSql() {
        return initialLoadSql;
    }

    public SelectFromTableOptions initialLoadSql(String initialLoadSql) {
        this.initialLoadSql = initialLoadSql;
        return this;
    }

    public int getExpectedCommaCount() {
        return expectedCommaCount;
    }

    public SelectFromTableOptions expectedCommaCount(int expectedCommaCount) {
        this.expectedCommaCount = expectedCommaCount;
        return this;
    }

    public boolean isSelectedAsCsv() {
        return selectedAsCsv;
    }

    public SelectFromTableOptions selectedAsCsv(boolean selectedAsCsv) {
        this.selectedAsCsv = selectedAsCsv;
        return this;
    }

    public boolean isObjectValuesWillNeedEscaped() {
        return objectValuesWillNeedEscaped;
    }

    public SelectFromTableOptions objectValuesWillNeedEscaped(boolean objectValuesWillNeedEscaped) {
        this.objectValuesWillNeedEscaped = objectValuesWillNeedEscaped;
        return this;
    }

    public boolean[] isColumnPositionUsingTemplate() {
        return columnPositionUsingTemplate;
    }

    public SelectFromTableOptions columnPositionUsingTemplate(boolean[] isColumnPositionUsingTemplate) {
        this.columnPositionUsingTemplate = isColumnPositionUsingTemplate;
        return this;
    }

    public boolean isCheckRowLength() {
        return checkRowLength;
    }

    public SelectFromTableOptions checkRowLength(boolean checkRowLength) {
        this.checkRowLength = checkRowLength;
        return this;
    }

    public long getRowMaxLength() {
        return rowMaxLength;
    }

    public SelectFromTableOptions rowMaxLength(long rowMaxLength) {
        this.rowMaxLength = rowMaxLength;
        return this;
    }

    public boolean isReturnLobObjects() {
        return returnLobObjects;
    }

    public SelectFromTableOptions returnLobObjects(boolean returnLobObjects) {
        this.returnLobObjects = returnLobObjects;
        return this;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public SelectFromTableOptions maxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return this;
    }
}
