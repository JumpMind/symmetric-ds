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
package org.jumpmind.symmetric.route;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;

/**
 * This is a router that is tied to the trigger table. It prevents triggers from
 * being routed to pre-2.0 versions of SymmetricDS.
 */
public class TriggerDataRouter extends AbstractDataRouter {

    public Collection<String> routeToNodes(IRouterContext context, DataMetaData dataMetaData,
            Set<Node> nodes, boolean initialLoad) {
        Set<String> nodeIds = new HashSet<String>();
        if (!initialLoad) {
            for (Node node : nodes) {
                String version = node.getSymmetricVersion();
                if (version != null) {
                    int max = Version.parseVersion(version)[Version.MAJOR_INDEX];
                    if (max >= 2) {
                        nodeIds.add(node.getNodeId());
                    }
                }
            }
            return nodeIds;
        } else {
            return toNodeIds(nodes, nodeIds);
        }
    }

    public boolean isAutoRegister() {
        return false;
    }

}