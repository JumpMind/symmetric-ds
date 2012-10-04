package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.impl.LoadFilterService.LoadFilterNodeGroupLink;

public interface ILoadFilterService {

    public Map<String, List<LoadFilter>> findLoadFiltersFor(NodeGroupLink link, boolean useCache);

    public List<LoadFilterNodeGroupLink> getLoadFilterNodeGroupLinks();

    public void saveLoadFilter(LoadFilterNodeGroupLink loadFilter);

    public void deleteLoadFilter(String loadFilterId);

    public void resetCache();

}
