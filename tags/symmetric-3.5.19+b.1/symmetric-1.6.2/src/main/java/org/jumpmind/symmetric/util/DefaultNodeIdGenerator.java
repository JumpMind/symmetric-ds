package org.jumpmind.symmetric.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;

public class DefaultNodeIdGenerator implements INodeIdGenerator {

    public boolean isAutoRegister() {
        return true;
    }

    public String selectNodeId(INodeService nodeService, Node node) {
        if (StringUtils.isBlank(node.getNodeId())) {
            String nodeId = buildNodeId(nodeService, node);
            int maxTries = 100;
            for (int sequence = 0; sequence < maxTries; sequence++) {
                NodeSecurity security = nodeService.findNodeSecurity(nodeId);
                if (security != null && security.isRegistrationEnabled()) {
                    return nodeId;
                }
                nodeId = buildNodeId(nodeService, node) + "-" + sequence;
            }
        }
        return node.getNodeId();
    }

    public String generateNodeId(INodeService nodeService, Node node) {
        if (StringUtils.isBlank(node.getNodeId())) {
            String nodeId = buildNodeId(nodeService, node);
            int maxTries = 100;
            for (int sequence = 0; sequence < maxTries; sequence++) {
                if (nodeService.findNode(nodeId) == null) {
                    return nodeId;
                }
                nodeId = buildNodeId(nodeService, node) + "-" + sequence;
            }
            throw new RuntimeException("Could not find nodeId for externalId of " + node.getExternalId() + " after "
                    + maxTries + " tries.");
        } else {
            return node.getNodeId();
        }
    }

    protected String buildNodeId(INodeService nodeService, Node node) {
        return node.getExternalId();
    }

    public String generatePassword(INodeService nodeService, Node node) {
        return new RandomDataImpl().nextSecureHexString(30);
    }
}