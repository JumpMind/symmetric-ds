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
package org.jumpmind.symmetric.web.rest.model;

public class RegistrationInfo {

    /**
     * The nodeId that was generated during the registration process for the given node
     * based on its external id
     */
	String nodeId;
	
	/**
	 * The URL that should be used to request (pull) data in the sycnronization scenario
	 */
	String syncUrl;
	
	/**
	 * Returns the node id that was generated during the registration process for the given node
	 * based on the external id passed into the registration process.
	 * @return
	 */
	public String getNodeId() {
		return nodeId;
	}
	
	/**
	 * Setter for the node id field
	 * @param nodeId
	 */
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	
	/**
	 * Returns the root synchronization url that should be used for subsequent REST service requests such as /engine/pulldata
	 * @return
	 */
	public String getSyncUrl() {
		return syncUrl;
	}
	
	/**
	 * Setter for the sync url field
	 * @param syncUrl
	 */
	public void setSyncUrl(String syncUrl) {
		this.syncUrl = syncUrl;
	}
}
