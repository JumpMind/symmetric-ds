package org.jumpmind.symmetric.io.data.writer;

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.symmetric.io.data.ConflictException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.ConflictEvent.Status;
import org.jumpmind.symmetric.io.data.writer.ConflictSetting.ResolveInsertConflict;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriterConflictResolver implements IDatabaseWriterConflictResolver {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected boolean autoRegister;

    protected void reportConflict(DatabaseWriter writer, DatabaseWriterSettings writerSettings,
            CsvData data, ConflictEvent conflictEvent) {

    }

    public void needsResolved(DatabaseWriter writer, DatabaseWriterSettings writerSettings,
            CsvData data) {
        DataEventType originalEventType = data.getDataEventType();
        ConflictSetting conflictSetting = writerSettings.getConflictSettings(
                writer.getTargetTable(), writer.getBatch());
        switch (originalEventType) {
            case INSERT:
                switch (conflictSetting.getResolveInsertType()) {
                    case MANUAL:
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.ER, data, null));
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    case BLIND_FALLBACK:
                        try {
                            performFallbackToUpdate(writer, data);
                            reportConflict(
                                    writer,
                                    writerSettings,
                                    data,
                                    new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                            .getTargetTable(), Status.OK, data,
                                            ResolveInsertConflict.BLIND_FALLBACK.name()));

                        } catch (RuntimeException ex) {
                            reportConflict(
                                    writer,
                                    writerSettings,
                                    data,
                                    new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                            .getTargetTable(), Status.ER, data, ex.getMessage()));
                            throw ex;
                        }
                        break;
                    case NEWER_WINS:
                        // TODO
                        throw new NotImplementedException();
                    case IGNORE:
                    default:
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.OK, data,
                                        ResolveInsertConflict.IGNORE.name()));
                        break;
                }
                break;

            case UPDATE:
                switch (conflictSetting.getResolveUpdateType()) {
                    case MANUAL:
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.ER, data, null));
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    case BLIND_FALLBACK:
                        try {
                            performFallbackToInsert(writer, data);
                            reportConflict(
                                    writer,
                                    writerSettings,
                                    data,
                                    new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                            .getTargetTable(), Status.OK, data,
                                            ResolveInsertConflict.BLIND_FALLBACK.name()));

                        } catch (RuntimeException ex) {
                            reportConflict(
                                    writer,
                                    writerSettings,
                                    data,
                                    new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                            .getTargetTable(), Status.ER, data, ex.getMessage()));
                            throw ex;
                        }
                        break;
                    case NEWER_WINS:
                        // TODO
                        throw new NotImplementedException();
                    case IGNORE:
                    default:
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.OK, data,
                                        ResolveInsertConflict.IGNORE.name()));
                        break;
                }
                break;

            case DELETE:
                switch (conflictSetting.getResolveDeleteType()) {
                    case MANUAL:
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.ER, data, null));
                        throw new ConflictException(data, writer.getTargetTable(), false);
                    default:
                    case IGNORE:
                        writer.getStatistics().get(writer.getBatch())
                                .increment(DataWriterStatisticConstants.MISSINGDELETECOUNT);
                        reportConflict(
                                writer,
                                writerSettings,
                                data,
                                new ConflictEvent(writer.getBatch(), conflictSetting, writer
                                        .getTargetTable(), Status.OK, data,
                                        ResolveInsertConflict.IGNORE.name()));
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
