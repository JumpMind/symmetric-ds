package org.jumpmind.symmetric.transform;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.util.CsvUtils;

public class TransformDataExtractor extends AbstractTransformer implements IExtractorFilter {

    protected final String DATA_KEY = "DATA_KEY-" + hashCode();

    public boolean filterData(Data data, String routerId, DataExtractorContext context) {
        DataEventType eventType = data.getEventType();
        DmlType dmlType = toDmlType(eventType);
        if (dmlType != null) {
            context.getContextCache().put(DATA_KEY, data);
            TriggerHistory triggerHistory = data.getTriggerHistory();
            transform(dmlType, context, triggerHistory.getSourceCatalogName(),
                    triggerHistory.getSourceSchemaName(), data.getTableName(),
                    triggerHistory.getParsedColumnNames(), data.toParsedRowData(),
                    triggerHistory.getParsedPkColumnNames(), data.toParsedPkData(),
                    data.toParsedOldData());
        }
        return true;
    }

    @Override
    protected void apply(ICacheContext context, TransformedData transformedData) {
        Data data = (Data) context.getContextCache().get(DATA_KEY);
        TriggerHistory triggerHistory = data.getTriggerHistory();

        DmlType targetDmlType = transformedData.getTargetDmlType();
        if (targetDmlType != null) {
            String[] columnNames = transformedData.getColumnNames();
            triggerHistory.setColumnNames(CsvUtils.escapeCsvData(columnNames));

            String[] columnValues = transformedData.getColumnValues();
            data.setRowData(CsvUtils.escapeCsvData(columnValues));

            String[] keyNames = transformedData.getKeyNames();
            triggerHistory.setPkColumnNames(CsvUtils.escapeCsvData(keyNames));

            String[] keyValues = transformedData.getKeyValues();
            data.setPkData(CsvUtils.escapeCsvData(keyValues));

            data.setOldData(null);

        }
    }

    protected DmlType toDmlType(DataEventType eventType) {
        switch (eventType) {
        case INSERT:
            return DmlType.INSERT;
        case UPDATE:
            return DmlType.UPDATE;
        case DELETE:
            return DmlType.DELETE;
        default:
            return null;
        }
    }

    protected DataEventType toDataEventType(DmlType dmlType) {
        switch (dmlType) {
        case INSERT:
            return DataEventType.INSERT;
        case UPDATE:
            return DataEventType.UPDATE;
        case DELETE:
            return DataEventType.DELETE;
        default:
            return null;
        }
    }


}
