package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.INodeGroupDataLoaderFilter;

public class NodeGroupTestDataLoaderFilter implements INodeGroupDataLoaderFilter {

    protected int numberOfTimesCalled = 0;
    
    String[] nodeGroups;
    
    public String[] getNodeGroupIdsToApplyTo() {
        return nodeGroups;
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        numberOfTimesCalled++;
        return true;
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void setNodeGroups(String[] nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public int getNumberOfTimesCalled() {
        return numberOfTimesCalled;
    }

}
