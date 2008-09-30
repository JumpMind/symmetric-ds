package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.ext.INodeGroupExtensionPoint;

/**
 * This is a DataLoaderFilter that will only be applied to the node groups that
 * are supported.
 */
public interface INodeGroupDataLoaderFilter extends IDataLoaderFilter, INodeGroupExtensionPoint {

}
