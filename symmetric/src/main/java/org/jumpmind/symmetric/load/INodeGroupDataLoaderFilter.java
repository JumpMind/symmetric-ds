package org.jumpmind.symmetric.load;

/**
 * This is a DataLoaderFilter that will only be applied to the node groups that are supported.
 */
public interface INodeGroupDataLoaderFilter extends IDataLoaderFilter {

    public String[] getNodeGroupIdsToApplyTo();
    
}
