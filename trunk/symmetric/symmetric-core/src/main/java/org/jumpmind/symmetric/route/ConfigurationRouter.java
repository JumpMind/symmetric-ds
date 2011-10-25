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
package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.Set;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;

public class ConfigurationRouter implements IDataRouter {

    public boolean isAutoRegister() {
        return true;
    }

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        // This guy is going to replace the initial load selects for sym_node and sym_node_security found in 
        // triggerrouter-service.xml.  The nodes variable has all eligible nodes that can be sync'd to.
        // Go through them all and figure out if the sym_node or sym_node_security rows should be synced.  If so,
        // return the nodeid in the returned collection.
        
        // if the configuration table is something other than node or security, then return all node ids (configuration
        // goes everywhere.
        
        // this router is configured in symmetric-routers.xml.  it will be used in TriggerRouterService.buildRegistrationTriggerRouter()
        // we can get rid of rootConfigChannelInitialLoadSelect in triggerrouter-service.xml
        
        // side note: if the external id of a node exists in registration_redirect, then we should sync that node only
        // to the registration_node_id.
        
        // another other side node:  we should put some indicator into the context if sym_trigger, sym_trigger_router, or sym_router
        // changes so we can run syncTriggers when the batch is completed.
        
        return null;
    }

    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
        // TODO resync triggers if sym_trigger, sym_trigger_router or sym_router has changed
    }

    public void contextCommitted(IRouterContext context) {
    }

}
