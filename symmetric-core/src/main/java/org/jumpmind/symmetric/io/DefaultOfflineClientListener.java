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
        log.warn("Synchronization is disabled on the server node");
        nodeService.deleteIdentity();
    }
    
    public void registrationRequired(Node remoteNode) {
        log.warn("Registration is required before this operation can complete");
    }
    
}
