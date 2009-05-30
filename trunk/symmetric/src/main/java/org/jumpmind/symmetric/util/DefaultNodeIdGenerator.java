/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Mark Hanes <eegeek@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
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