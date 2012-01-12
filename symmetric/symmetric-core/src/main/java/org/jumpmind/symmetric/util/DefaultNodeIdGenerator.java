/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;

/**
 * 
 */
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
            
            return nodeId;

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
        return StringUtils.isBlank(node.getExternalId()) ? "0" : node.getExternalId();
    }

    public String generatePassword(INodeService nodeService, Node node) {
        return new RandomDataImpl().nextSecureHexString(30);
    }
}