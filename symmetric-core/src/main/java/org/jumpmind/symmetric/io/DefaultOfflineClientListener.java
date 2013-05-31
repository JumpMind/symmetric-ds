package org.jumpmind.symmetric.io;

import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of an {@link IOfflineClientListener}.  When the listener detects
 * that sync has been disabled or registration is required, the local node identity is removed.
 */
public class DefaultOfflineClientListener implements IOfflineClientListener, IBuiltInExtensionPoint {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected IParameterService parameterService;
    protected INodeService nodeService;
    
    public DefaultOfflineClientListener(IParameterService parameterService,
            INodeService nodeService) {
        this.parameterService = parameterService;
        this.nodeService = nodeService;
    }

    public void busy(Node remoteNode) {
        log.warn("Node '{}' was too busy to accept the connection", remoteNode.getNodeId());
    }

    public void notAuthenticated(Node remoteNode) {
        log.warn("Could not authenticate with node '{}'", remoteNode.getNodeId());
    }
    
    public void unknownError(Node remoteNode, Exception ex) {
    }

    public void offline(Node remoteNode) {
        log.warn("Could not connect to the transport: {}",
                (remoteNode.getSyncUrl() == null ? parameterService.getRegistrationUrl() : remoteNode
                        .getSyncUrl()));
    }

    public void syncDisabled(Node remoteNode) {
        Node identity = nodeService.findIdentity();
        if (identity != null && identity.getCreatedAtNodeId() != null
                && identity.getCreatedAtNodeId().equals(remoteNode.getNodeId())) {
            log.warn("Removing identity because sync has been disabled");
            nodeService.deleteIdentity();
        }
    }
    
    public void registrationRequired(Node remoteNode) {
        log.warn("Registration is required before this operation can complete");
    }
    
}
