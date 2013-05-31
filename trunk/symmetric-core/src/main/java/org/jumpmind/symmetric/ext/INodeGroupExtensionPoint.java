package org.jumpmind.symmetric.ext;

/**
 * This is a DataLoaderFilter that will only be applied to the node groups that
 * are supported.
 *
 * 
 */
public interface INodeGroupExtensionPoint {

    public String[] getNodeGroupIdsToApplyTo();

}