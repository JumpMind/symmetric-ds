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

import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.impl.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;

public class DefaultNodeIdGenerator implements INodeIdGenerator {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected IParameterService parameterService;

    public DefaultNodeIdGenerator(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public String selectNodeId(INodeService nodeService, Node node) {
        final int maxTries = parameterService.getInt(ParameterConstants.NODE_ID_GENERATOR_MAX_NODES, 100);
        if (StringUtils.isBlank(node.getNodeId())) {
            String nodeId = evaluateScript(node);
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

    public String generateNodeId(INodeService nodeService, Node node) {
        final int maxTries = parameterService.getInt(ParameterConstants.NODE_ID_GENERATOR_MAX_NODES, 100);
        String nodeId = node.getNodeId();
        if (StringUtils.isBlank(nodeId)) {
            nodeId = evaluateScript(node);
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

    public String generatePassword(INodeService nodeService, Node node) {
        return new SecurityService().nextSecureHexString(30);
    }
 
    protected String evaluateScript(Node node) {
        String script = parameterService.getString(ParameterConstants.NODE_ID_GENERATOR_SCRIPT);
        if (StringUtils.isNotBlank(script)) {
            try {
                Interpreter interpreter = new Interpreter();
                interpreter.set("node", node);
                try {
                    URL url = new URL(node.getSyncUrl());
                    interpreter.set("hostname", url.getHost());
                } catch (Exception ex) {
                    interpreter.set("hostname", null);
                }
                Object retValue = interpreter.eval(script);
                if (retValue != null) {
                    return retValue.toString();
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