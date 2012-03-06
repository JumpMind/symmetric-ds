package org.jumpmind.symmetric.io.data.writer;

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.symmetric.io.data.ConflictException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected boolean autoRegister;
    
    public void reportConflict(DatabaseWriter writer, DatabaseWriterSettings writerSettings,
            CsvData data, ConflictEvent conflictEvent) {
        
    }

    public void needsResolved(DatabaseWriter writer, DatabaseWriterSettings writerSettings,
            CsvData data) {
        DataEventType originalEventType = data.getDataEventType();
        ConflictSettings conflictSetting = writerSettings.getConflictSettings(
                writer.getTargetTable(), writer.getBatch());
        switch (originalEventType) {
            case INSERT:
                switch (conflictSetting.getResolveInsertType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    case BLINK_FALLBACK:
                        performFallbackToUpdate(writer, data);
                        break;
                    case NEWER_WINS:
                        // TODO
                        throw new NotImplementedException();
                    case IGNORE:
                    default:
                        break;
                }
                break;

            case UPDATE:
                switch (conflictSetting.getResolveUpdateType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    case BLINK_FALLBACK:
                        performFallbackToInsert(writer, data);
                        break;
                    case NEWER_WINS:
                        // TODO
                        throw new NotImplementedException();
                    case IGNORE:
                    default:
                        break;
                }
                break;

            case DELETE:
                switch (conflictSetting.getResolveDeleteType()) {
                    case MANUAL:
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    default:
                    case IGNORE:
                        writer.getStatistics().get(writer.getBatch())
                                .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
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

}
