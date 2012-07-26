package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.NodeGroupLink;

public interface ILoadFilterService {

    public Map<String, List<LoadFilter>> findLoadFiltersFor(NodeGroupLink link, boolean useCache);

    public List<LoadFilter> getLoadFilters();

    public void saveLoadFilter(LoadFilter loadFilter);

    public void deleteLoadFilter(String loadFilterId);

    public void resetCache();

}
