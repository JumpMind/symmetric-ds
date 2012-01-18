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

package org.jumpmind.symmetric.transport.handler;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

/**
 * 
 */
public class AuthenticationResourceHandler extends AbstractTransportResourceHandler {

    public enum AuthenticationStatus {
        SYNC_DISABLED, REGISTRATION_REQUIRED, FORBIDDEN, ACCEPTED;
    };

    private INodeService nodeService;

    public AuthenticationStatus status(String nodeId, String securityToken) {
        AuthenticationStatus retVal = AuthenticationStatus.ACCEPTED;
        if (nodeService.findNode(nodeId) == null) {
          retVal = AuthenticationStatus.REGISTRATION_REQUIRED;
        } else if (!syncEnabled(nodeId)) {
          retVal = AuthenticationStatus.SYNC_DISABLED; 
        } else if (!nodeService.isNodeAuthorized(nodeId, securityToken)) {
          retVal = AuthenticationStatus.FORBIDDEN;
        }
        return retVal;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
    protected boolean syncEnabled(String nodeId) {
        boolean syncEnabled = false;
        Node node = nodeService.findNode(nodeId);
        if (node != null) {
            syncEnabled = node.isSyncEnabled();
        }
        return syncEnabled;
    }

}