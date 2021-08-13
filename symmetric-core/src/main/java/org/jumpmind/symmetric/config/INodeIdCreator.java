/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.config;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.util.DefaultNodeIdCreator;

/**
 * An {@link IExtensionPoint} that allows SymmetricDS users to implement their own algorithms for how node_ids and passwords are generated or selected during
 * registration. There may be only one node creator per SymmetricDS instance.
 * </p>
 * The default implementation of this is the {@link DefaultNodeIdCreator}
 */
public interface INodeIdCreator extends IExtensionPoint {
    /**
     * Based on the node parameters passed in generate an expected node id. This is used in an attempt to match a registration request with an open
     * registration.
     */
    public String selectNodeId(Node node, String remoteHost, String remoteAddress);

    /**
     * Based on the node parameters passed in generate a brand new node id.
     */
    public String generateNodeId(Node node, String remoteHost, String remoteAddress);

    /**
     * Generate a password to use when opening registration
     */
    public String generatePassword(Node node);
}
