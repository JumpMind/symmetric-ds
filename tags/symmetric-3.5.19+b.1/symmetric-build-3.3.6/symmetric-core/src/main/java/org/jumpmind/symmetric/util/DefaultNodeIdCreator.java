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
 * under the License. 
 */

package org.jumpmind.symmetric.util;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.security.SecurityService;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.TargetError;

public class DefaultNodeIdCreator implements INodeIdCreator {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IParameterService parameterService;
    
    protected INodeService nodeService;

    public DefaultNodeIdCreator(IParameterService parameterService, INodeService nodeService) {
        this.parameterService = parameterService;
        this.nodeService = nodeService;
    }        
    
    public String selectNodeId(Node node, String remoteHost, String remoteAddress) {
        final int maxTries = parameterService.getInt(ParameterConstants.NODE_ID_CREATOR_MAX_NODES, 100);
        if (StringUtils.isBlank(node.getNodeId())) {
            String nodeId = evaluateScript(node, remoteHost, remoteAddress);
            if (StringUtils.isBlank(nodeId)) {
                nodeId = buildNodeId(nodeService, node);
                for (int sequence = 0; sequence < maxTries; sequence++) {
                    NodeSecurity security = nodeService.findNodeSecurity(nodeId);
                    if (security != null && security.isRegistrationEnabled()) {
                        return nodeId;
                    }
                    nodeId = buildNodeId(nodeService, node) + "-" + sequence;
                }
            }
            return nodeId;

        }
        return node.getNodeId();
    }

    public String generateNodeId(Node node, String remoteHost, String remoteAddress) {
        final int maxTries = parameterService.getInt(ParameterConstants.NODE_ID_CREATOR_MAX_NODES, 100);
        String nodeId = node.getNodeId();
        if (StringUtils.isBlank(nodeId)) {
            nodeId = evaluateScript(node, remoteHost, remoteAddress);
            if (StringUtils.isBlank(nodeId)) {
                nodeId = buildNodeId(nodeService, node);
                for (int sequence = 0; sequence < maxTries; sequence++) {
                    if (nodeService.findNode(nodeId) == null) {                        
                        break;
                    }
                    nodeId = buildNodeId(nodeService, node) + "-" + sequence;
                }
                
                if (nodeService.findNode(nodeId) != null) {
                    nodeId = null;
                }
            }
        }

        if (StringUtils.isNotBlank(nodeId)) {
            return nodeId;
        } else {
            throw new RuntimeException("Could not find nodeId for externalId of "
                    + node.getExternalId() + " after " + maxTries + " tries.");
        }
    }

    protected String buildNodeId(INodeService nodeService, Node node) {
        return StringUtils.isBlank(node.getExternalId()) ? "0" : node.getExternalId();
    }
    
    public String generatePassword(Node node) {
        return new SecurityService().nextSecureHexString(30);
    }
 
    protected String evaluateScript(Node node, String remoteHost, String remoteAddress) {
        String script = parameterService.getString(ParameterConstants.NODE_ID_CREATOR_SCRIPT);
        if (StringUtils.isNotBlank(script)) {
            try {
                Interpreter interpreter = new Interpreter();
                interpreter.set("node", node);
                interpreter.set("hostname", remoteHost);
                interpreter.set("remoteHost", remoteHost);
                interpreter.set("remoteAddress", remoteAddress);
                Object retValue = interpreter.eval(script);
                if (retValue != null) {
                    return retValue.toString();
                }
            } catch (TargetError e) {
                if (e.getTarget() instanceof RuntimeException) {
                    throw (RuntimeException)e.getTarget();
                } else {
                    throw new RuntimeException(e.getTarget() != null ? e.getTarget() : e);
                }
            } catch (EvalError e) {
                log.error(
                        "Failed to evalute node id generator script.  The default node id generation mechanism will be used.",
                        e);
            }
        }
        return null;
    }
}