package org.jumpmind.symmetric.transform;

import java.util.List;
import java.util.Map;

public interface ITransformService {
    
    public Map<String, List<TransformTable>> findTransformsFor(String nodeGroupId, boolean useCache);

}
