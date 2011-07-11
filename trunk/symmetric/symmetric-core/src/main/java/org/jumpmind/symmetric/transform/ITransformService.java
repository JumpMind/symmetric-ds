package org.jumpmind.symmetric.transform;

import java.util.List;
import java.util.Map;

public interface ITransformService {
    
    public Map<String, List<TransformTable>> findTransformsFor(String nodeGroupId, boolean useCache);
    
    public List<TransformTable> getTransformTables();
    
    public List<TransformColumn> getTransformColumns();
    
    public List<TransformColumn> getTransformColumnsForTable();
    
    public void saveTransformTable(TransformTable transformTable);        
    
    public void deleteTransformTable(String transformTableId);
    
    /* methods for transform columns */

    public void saveTransformColumn(TransformColumn transformColumn);
    
    public void deleteTransformColumn(String transformTableId, Boolean includeOn, String targetColumnName);
        
}
