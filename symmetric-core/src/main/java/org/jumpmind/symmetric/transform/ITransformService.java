package org.jumpmind.symmetric.transform;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.NodeGroupLink;

public interface ITransformService {

    public Map<String, IColumnTransform<?>> getColumnTransforms();

    public void addColumnTransform(String name, IColumnTransform<?> transform);

    public Map<String, List<TransformTable>> findTransformsFor(NodeGroupLink link,
            TransformPoint transformPoint, boolean useCache);

    public List<TransformTable> getTransformTables();

    public List<TransformColumn> getTransformColumns();

    public List<TransformColumn> getTransformColumnsForTable();

    public void saveTransformTable(TransformTable transformTable);

    public void deleteTransformTable(String transformTableId);

    public void saveTransformColumn(TransformColumn transformColumn);

    public void deleteTransformColumn(String transformTableId, Boolean includeOn,
            String targetColumnName);

    public void resetCache();

}
