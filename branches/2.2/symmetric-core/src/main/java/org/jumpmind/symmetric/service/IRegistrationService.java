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


package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.RegistrationRequest;
import org.jumpmind.symmetric.security.INodePasswordFilter;

/**
 * This service provides an API that deals with {@link Node} registration
 */
public interface IRegistrationService {

    /**
     * Register a node for the given group name and external id if the
     * registration is open.
     * 
     * @param isRequestedRegistration
     *            An indicator that registration has been requested by the
     *            remote client
     */
    public boolean registerNode(Node node, String remoteHost, String remoteAddress, OutputStream out, boolean isRequestedRegistration) throws IOException;

    /**
     * Register a node for the given group name and external id if the
     * registration is open.
     * 
     * @param isRequestedRegistration
     *            An indicator that registration has been requested by the
     *            remote client
     */
    public boolean registerNode(Node node, OutputStream out, boolean isRequestedRegistration) throws IOException;

    /**
     * Open registration for a single new node given a node group (f.e.,
     * "STORE") and external ID (f.e., "00001"). The unique node ID and password
     * are generated and stored in the node and node_security tables with the
     * registration_enabled flag turned on. The next node to try registering for
     * this node group and external ID will be given this information.
     * @return the node id
     */
    public String openRegistration(String nodeGroupId, String externalId);

    /**
     * Re-open registration for a single node that already exists in the
     * database. A new password is generated and the registration_enabled flag
     * is turned on. The next node to try registering for this node group and
     * external ID will be given this information.
     */
    public void reOpenRegistration(String nodeId);

    /**
     * Mark the passed in node as registered in node_security
     * @param nodeId is the node that has just finished 'successfully' registering
     */
    public void markNodeAsRegistered(String nodeId);
    
    public boolean isAutoRegistration();

    /**
     * Client method which attempts to register with the registration.url to
     * pull configuration if the node has not already been registered. If the
     * registration server cannot be reach this method will continue to try with
     * random sleep periods up to one minute up until the registration succeeds
     * or the maximum number of attempts has been reached.
     */
    public void registerWithServer();
    
    public List<RegistrationRequest> getRegistrationRequests(boolean includeNodesWithOpenRegistrations);
    
    public void saveRegisgtrationRequest(RegistrationRequest request);

    public boolean isRegisteredWithServer();
    
    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter);
    
    /**
     * Add an entry to the registation_redirect table so that if a node tries to register here.  It will be redirected to the correct node.
     */
    public void saveRegistrationRedirect(String externalIdToRedirect, String nodeIdToRedirectTo);
    
    /**
     * @return a map of nodes to redirect to that is keyed by a list of external_ids that should be redirected.
     */
    public Map<String,String> getRegistrationRedirectMap();
    
    public String getRedirectionUrlFor(String externalId);
       

}