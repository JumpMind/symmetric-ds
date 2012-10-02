package org.jumpmind.symmetric.io.data.writer;

import java.util.List;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.transform.TransformedData;

public class DefaultTransformWriterConflictResolver extends DefaultDatabaseWriterConflictResolver {

    protected TransformWriter transformWriter;

    public DefaultTransformWriterConflictResolver(TransformWriter transformWriter) {
        this.transformWriter = transformWriter;
    }

    @Override
    protected void performFallbackToInsert(DatabaseWriter writer, CsvData data, Conflict conflict) {
        TransformedData transformedData = data.getAttribute(TransformedData.class.getName());
        if (transformedData != null) {
            List<TransformedData> newlyTransformedDatas = transformWriter.transform(
                    DataEventType.INSERT, writer.getContext(), transformedData.getTransformation(),
                    transformedData.getSourceKeyValues(), transformedData.getOldSourceValues(),
                    transformedData.getSourceValues());
            for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                if (newlyTransformedData.hasSameKeyValues(transformedData.getKeyValues())
                        || newlyTransformedData.isGeneratedIdentityNeeded()) {
                    Table table = newlyTransformedData.buildTargetTable();
                    CsvData newData = newlyTransformedData.buildTargetCsvData();
                    String quote = writer.getPlatform().getDatabaseInfo().getDelimiterToken();
                    if (newlyTransformedData.isGeneratedIdentityNeeded()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Enabling generation of identity for {}",
                                    newlyTransformedData.getTableName());
                        }
                        writer.getTransaction().allowInsertIntoAutoIncrementColumns(false, table, quote);
                    } else if (table.hasAutoIncrementColumn()) {
                        writer.getTransaction().allowInsertIntoAutoIncrementColumns(true, table, quote);
                    }

                    writer.start(table);
                    writer.write(newData);
                    writer.end(table);
                }
            }
        } else {
            super.performFallbackToInsert(writer, data, conflict);
        }
    }

    @Override
    protected void performFallbackToUpdate(DatabaseWriter writer, CsvData data, Conflict conflict) {
        TransformedData transformedData = data.getAttribute(TransformedData.class.getName());
        if (transformedData != null) {
            List<TransformedData> newlyTransformedDatas = transformWriter.transform(
                    DataEventType.UPDATE, writer.getContext(), transformedData.getTransformation(),
                    transformedData.getSourceKeyValues(), transformedData.getOldSourceValues(),
                    transformedData.getSourceValues());
            for (TransformedData newlyTransformedData : newlyTransformedDatas) {
                if (newlyTransformedData.hasSameKeyValues(transformedData.getKeyValues())) {
                    Table table = newlyTransformedData.buildTargetTable();
                    writer.start(table);
                    writer.write(newlyTransformedData.buildTargetCsvData());
                    writer.end(table);
                }
            }
        } else {
            super.performFallbackToUpdate(writer, data, conflict);
        }
    }

}
