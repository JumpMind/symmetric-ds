package org.jumpmind.symmetric.io;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that is called when the current instance has detected it cannot sync with another
 * {@link Node}.
 */
public interface IOfflineClientListener extends IExtensionPoint {

    /**
     * Called when the remote node is unreachable.
     */
    public void offline(Node remoteNode);

    /**
     * Called when this node is rejected because of a password mismatch.
     */
    public void notAuthenticated(Node remoteNode);

    /**
     * Called when this node has been rejected because the remote node is currently too busy to handle the sync request.
     */
    public void busy(Node remoteNode);
    
    /**
     * Called when this node is rejected because synchronization is disabled on the remote node.
     * 
     * @param remoteNode
     */
    public void syncDisabled(Node remoteNode);

    /**
     * Called when this node is rejected because the node has not been registered with the remote node.
     * @param remoteNode
     */
    public void registrationRequired(Node remoteNode);
    
    public void unknownError(Node remoteNode, Exception ex);

}