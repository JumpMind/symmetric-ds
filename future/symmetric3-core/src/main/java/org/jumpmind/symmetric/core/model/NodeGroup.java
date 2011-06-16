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


package org.jumpmind.symmetric.core.model;

import java.io.Serializable;

public class NodeGroup implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String nodeGroupId;

    private String description;

    public NodeGroup(String nodeGroupId) {
        this.nodeGroupId = nodeGroupId;
    }
    
    public NodeGroup() {
    }
    

    public String getNodeGroupId() {
        return nodeGroupId;
    }

    public void setNodeGroupId(String name) {
        this.nodeGroupId = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}