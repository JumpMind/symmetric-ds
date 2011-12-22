package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.io.data.transform.TransformColumn;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;

public interface ITransformService {

    public List<TransformTableNodeGroupLink> findTransformsFor(NodeGroupLink link,
            TransformPoint transformPoint, boolean useCache);

    public List<TransformTableNodeGroupLink> getTransformTables();

    public List<TransformColumn> getTransformColumns();

    public List<TransformColumn> getTransformColumnsForTable();

    public void saveTransformTable(TransformTableNodeGroupLink transformTable);

    public void deleteTransformTable(String transformTableId);

    public void saveTransformColumn(TransformColumn transformColumn);

    public void deleteTransformColumn(String transformTableId, Boolean includeOn,
            String targetColumnName);

    public void resetCache();

}
