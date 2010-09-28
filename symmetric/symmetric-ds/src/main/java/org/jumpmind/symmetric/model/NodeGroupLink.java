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


package org.jumpmind.symmetric.model;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 */
public class NodeGroupLink {

    private static final long serialVersionUID = 1L;

    private String sourceNodeGroupId;

    private String targetNodeGroupId;

    private NodeGroupLinkAction dataEventAction = NodeGroupLinkAction.W;

    public NodeGroupLinkAction getDataEventAction() {
        return dataEventAction;
    }

    public void setDataEventAction(NodeGroupLinkAction dataEventAction) {
        this.dataEventAction = dataEventAction;
    }

    public String getSourceNodeGroupId() {
        return sourceNodeGroupId;
    }

    public void setSourceNodeGroupId(String domainName) {
        this.sourceNodeGroupId = domainName;
    }

    public String getTargetNodeGroupId() {
        return targetNodeGroupId;
    }

    public void setTargetNodeGroupId(String targetDomainName) {
        this.targetNodeGroupId = targetDomainName;
    }
}