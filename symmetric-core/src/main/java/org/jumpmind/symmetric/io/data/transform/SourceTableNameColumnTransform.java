package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.reader.ExtractDataReader;
import org.jumpmind.symmetric.model.Data;

public class SourceTableNameColumnTransform implements ISingleNewAndOldValueColumnTransform,
        IBuiltInExtensionPoint {

    @Override
    public String getName() {
        return "source_table_name";
    }

    @Override
    public NewAndOldValue transform(IDatabasePlatform platform, DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        NewAndOldValue value = new NewAndOldValue();
        Data csvData = (Data)context.get(ExtractDataReader.DATA_CONTEXT_CURRENT_CSV_DATA);
        if (csvData != null && csvData.getTriggerHistory() != null) {
            value.setNewValue(csvData.getTriggerHistory().getSourceTableName());
        }
        return value;
    }

    @Override
    public boolean isExtractColumnTransform() {
        return true;
    }

    @Override
    public boolean isLoadColumnTransform() {
        return false;
    }

}
