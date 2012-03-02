package org.jumpmind.symmetric.io.data.writer;

public class DatabaseWriterSettings {

    public enum ConflictResolutionInserts {
        IGNORE_CONTINUE, FALLBACK_UPDATE, ERROR_STOP
    };

    public enum ConflictResolutionUpdates {
        IGNORE_CONTINUE, FALLBACK_INSERT, ERROR_STOP
    };

    public enum ConflictResolutionDeletes {
        IGNORE_CONTINUE, ERROR_STOP
    };

    protected long maxRowsBeforeCommit = 10000;
    
    protected boolean treatDateTimeFieldsAsVarchar = false;
    protected boolean usePrimaryKeysFromSource = true;

    protected boolean useAllColumnsToIdentifyUpdateConflicts = false;

    protected ConflictResolutionInserts conflictResolutionInserts = ConflictResolutionInserts.FALLBACK_UPDATE;
    protected ConflictResolutionUpdates conflictResolutionUpdates = ConflictResolutionUpdates.FALLBACK_INSERT;
    protected ConflictResolutionDeletes conflictResolutionDeletes = ConflictResolutionDeletes.IGNORE_CONTINUE;

    public long getMaxRowsBeforeCommit() {
        return maxRowsBeforeCommit;
    }

    public void setMaxRowsBeforeCommit(long maxRowsBeforeCommit) {
        this.maxRowsBeforeCommit = maxRowsBeforeCommit;
    }

    public boolean isTreatDateTimeFieldsAsVarchar() {
        return treatDateTimeFieldsAsVarchar;
    }

    public void setTreatDateTimeFieldsAsVarchar(boolean treatDateTimeFieldsAsVarchar) {
        this.treatDateTimeFieldsAsVarchar = treatDateTimeFieldsAsVarchar;
    }

    public boolean isUsePrimaryKeysFromSource() {
        return usePrimaryKeysFromSource;
    }

    public void setUsePrimaryKeysFromSource(boolean usePrimaryKeysFromSource) {
        this.usePrimaryKeysFromSource = usePrimaryKeysFromSource;
    }

    public boolean isUseAllColumnsToIdentifyUpdateConflicts() {
        return useAllColumnsToIdentifyUpdateConflicts;
    }

    public void setUseAllColumnsToIdentifyUpdateConflicts(
            boolean useAllColumnsToIdentifyUpdateConflicts) {
        this.useAllColumnsToIdentifyUpdateConflicts = useAllColumnsToIdentifyUpdateConflicts;
    }

    public ConflictResolutionInserts getConflictResolutionInserts() {
        return conflictResolutionInserts;
    }

    public void setConflictResolutionInserts(ConflictResolutionInserts conflictResolutionInserts) {
        this.conflictResolutionInserts = conflictResolutionInserts;
    }

    public ConflictResolutionUpdates getConflictResolutionUpdates() {
        return conflictResolutionUpdates;
    }

    public void setConflictResolutionUpdates(ConflictResolutionUpdates conflictResolutionUpdates) {
        this.conflictResolutionUpdates = conflictResolutionUpdates;
    }

    public ConflictResolutionDeletes getConflictResolutionDeletes() {
        return conflictResolutionDeletes;
    }

    public void setConflictResolutionDeletes(ConflictResolutionDeletes conflictResolutionDeletes) {
        this.conflictResolutionDeletes = conflictResolutionDeletes;
    }

}
