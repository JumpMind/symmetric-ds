package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.util.CsvUtils;

public class TransformDataExtractor extends AbstractTransformer {

    public List<Data> transformData(Data data, String routerId, DataExtractorContext context)
            throws IgnoreRowException {
        DataEventType eventType = data.getEventType();
        DmlType dmlType = toDmlType(eventType);
        if (dmlType != null) {
            TriggerHistory triggerHistory = data.getTriggerHistory();
            List<TransformedData> transformedData = transform(dmlType, context,
                    triggerHistory.getSourceCatalogName(), triggerHistory.getSourceSchemaName(),
                    data.getTableName(), triggerHistory.getParsedColumnNames(),
                    data.toParsedRowData(),
                    dmlType == DmlType.INSERT ? null : triggerHistory.getParsedPkColumnNames(),
                    dmlType == DmlType.INSERT ? null : data.toParsedPkData(),
                    data.toParsedOldData());
            if (transformedData != null) {
                return apply(context, transformedData);
            }
        }

        return null;
    }

    protected List<Data> apply(ICacheContext context,
            List<TransformedData> dataThatHasBeenTransformed) {
        List<Data> datas = new ArrayList<Data>(dataThatHasBeenTransformed.size());
        for (TransformedData transformedData : dataThatHasBeenTransformed) {
            DmlType targetDmlType = transformedData.getTargetDmlType();
            if (targetDmlType != null) {
                Data data = new Data();
                TriggerHistory triggerHistory = new TriggerHistory(transformedData.getTableName(),
                        CsvUtils.escapeCsvData(transformedData.getKeyNames()),
                        CsvUtils.escapeCsvData(transformedData.getColumnNames()));
                triggerHistory.setSourceCatalogName(transformedData.getCatalogName());
                triggerHistory.setSourceSchemaName(transformedData.getSchemaName());
                data.setTriggerHistory(triggerHistory);
                data.setEventType(toDataEventType(transformedData.getTargetDmlType()));
                data.setTableName(transformedData.getTableName());
                data.setRowData(CsvUtils.escapeCsvData(transformedData.getColumnValues()));
                data.setPkData(CsvUtils.escapeCsvData(transformedData.getKeyValues()));
                data.setOldData(null);
                datas.add(data);
            }
        }
        return datas;
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
