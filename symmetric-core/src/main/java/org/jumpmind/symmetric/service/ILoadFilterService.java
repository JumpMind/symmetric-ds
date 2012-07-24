package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.impl.LoadFilterService.LoadFilterNodeGroupLink;

public interface ILoadFilterService {

    public List<LoadFilterNodeGroupLink> findLoadFiltersFor(NodeGroupLink link, boolean useCache);

    public List<LoadFilter> getLoadFilters();

    public void saveLoadFilter(LoadFilter loadFilter);

    public void deleteLoadFilter(String loadFilterId);

    public void resetCache();

}
