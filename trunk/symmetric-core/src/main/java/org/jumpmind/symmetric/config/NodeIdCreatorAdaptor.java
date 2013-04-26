package org.jumpmind.symmetric.config;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

@SuppressWarnings("deprecation")
public class NodeIdCreatorAdaptor implements INodeIdCreator {

    private INodeService nodeService;
    
    private INodeIdGenerator nodeIdGenerator;

    public NodeIdCreatorAdaptor(INodeIdGenerator generator, INodeService nodeService) {
        this.nodeIdGenerator = generator;
        this.nodeService = nodeService;
    }

    public String selectNodeId(Node node, String remoteHost, String remoteAddress) {
        return nodeIdGenerator.selectNodeId(nodeService, node);
    }

    public String generateNodeId(Node node, String remoteHost, String remoteAddress) {
        return nodeIdGenerator.generateNodeId(nodeService, node);
    }

    public String generatePassword(Node node) {
        return nodeIdGenerator.generatePassword(nodeService, node);
    }

}
