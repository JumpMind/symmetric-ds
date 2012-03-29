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

package org.jumpmind.symmetric.core.ext;

import org.jumpmind.symmetric.core.model.Node;
import org.jumpmind.symmetric.core.service.NodeService;

/**
 * An {@link IExtensionPoint} that allows SymmetricDS users to implement their
 * own algorithms for how node_ids and passwords are generated or selected
 * during registration. There may be only one node generator per SymmetricDS
 * instance. </p> The default implementation of this is the
 * {@link DefaultNodeIdGenerator}
 */
public interface INodeIdGenerator extends IExtensionPoint {

    /**
     * Based on the node parameters passed in generate an expected node id. This
     * is used in an attempt to match a registration request.
     */
    public String selectNodeId(NodeService nodeService, Node node);

    /**
     * Based on the node parameters passed in generate a brand new node id.
     */
    public String generateNodeId(NodeService nodeService, Node node);

    /**
     * Generate a password to use when opening registration
     */
    public String generatePassword(NodeService nodeService, Node node);
}