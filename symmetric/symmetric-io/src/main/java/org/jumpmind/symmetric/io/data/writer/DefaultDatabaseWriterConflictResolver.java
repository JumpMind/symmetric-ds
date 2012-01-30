package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.symmetric.io.data.ConflictException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {
    
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected boolean autoRegister;

    public void needsResolved(DatabaseWriter writer, CsvData data) {
        DataEventType originalEventType = data.getDataEventType();
        switch (originalEventType) {
        case INSERT:
            switch (writer.getTargetTableSettings().getConflictResolutionInserts()) {
            case ERROR_STOP:
                throw new ConflictException(data, writer.getTargetTable(), false);
            case FALLBACK_UPDATE:
                performFallbackToUpdate(writer, data);
                break;
            case IGNORE_CONTINUE:
            default:
                break;
            }
            break;

        case UPDATE:
            switch (writer.getTargetTableSettings().getConflictResolutionUpdates()) {
            case ERROR_STOP:
                throw new ConflictException(data, writer.getTargetTable(), false);
            case FALLBACK_INSERT:
                performFallbackToInsert(writer, data);
                break;
            case IGNORE_CONTINUE:
            default:
                break;
            }
            break;

        case DELETE:
            switch (writer.getTargetTableSettings().getConflictResolutionDeletes()) {
            case ERROR_STOP:
                throw new ConflictException(data, writer.getTargetTable(), false);
            case IGNORE_CONTINUE:
                writer.getStatistics().get(writer.getBatch())
                        .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
                break;
            default:
                break;
            }
            break;

        default:
            break;
        }
    }

    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data) {
        if (!writer.update(data)) {
            throw new ConflictException(data, writer.getTargetTable(), true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKUPDATECOUNT);
        }
    }

    protected void performFallbackToInsert(DatabaseWriter writer, CsvData csvData) {
        if (!writer.insert(csvData)) {
            throw new ConflictException(csvData, writer.getTargetTable(), true);
        } else {
            writer.getStatistics().get(writer.getBatch())
                    .increment(DataWriterStatisticConstants.FALLBACKINSERTCOUNT);
        }
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

}
