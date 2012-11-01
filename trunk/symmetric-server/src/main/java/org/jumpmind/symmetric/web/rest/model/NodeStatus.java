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

import org.jumpmind.symmetric.Version;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="nodestatus")
public class NodeStatus {
    
    boolean started;        

    /**
     * Is the node is registered with another node.
     */
	boolean registered;

	/**
	 * Is the node a registration server.
	 */
	boolean registrationServer;

	/**
	 * Is the node initially loaded.
	 */
	boolean initialLoaded;

	/**
	 * The node's ID.
	 */
	private String nodeId;

	/**
	 * The node's group ID.
	 */
	private String nodeGroupId;

	/**
	 * The node's external ID.
	 */
	private String externalId;

	/**
	 * The URL other nodes use to communicate with this node.
	 */
	private String syncUrl;

	/**
	 * The type of database the node connects to. (e.g., 'PostgreSQL')
	 */
	private String databaseType;

	/**
	 * The version of the database the node connects to. (e.g., '9.2')
	 */
	private String databaseVersion;

	/**
	 * Is the node enabled for synchronization.
	 */
	private boolean syncEnabled = true;

	/**
	 * The node ID where this node was created.
	 */
	private String createdAtNodeId;

	/**
	 * The number of batches waiting to be sent.
	 */
	private int batchToSendCount;

	/**
	 * The number of batches in the error state.
	 */
	private int batchInErrorCount;

	/**
	 * The node's SymmetricDS installation type. (e.g., 'professional')
	 */
	private String deploymentType;
	
	/**
	 * The version of SymmetricDS installed on the node. (e.g., '3.2.0-SNAPSHOT')
	 */
	private String symmetricVersion = Version.version();

	/**
	 * @return boolean indicating if the node is registered with another node.
	 */
	public boolean getRegistered() {
		return registered;
	}

	/**
	 * @param registered boolean indicating if the node is registered with another node.
	 */
	public void setRegistered(boolean registered) {
		this.registered = registered;
	}

	/** 
	 * @return boolean indicating if the node is a registration server.
	 */
	public boolean getRegistrationServer() {
		return registrationServer;
	}

	/**
	 * @param registrationServer boolean indicating if the node is a registration server.
	 */
	public void setRegistrationServer(boolean registrationServer) {
		this.registrationServer = registrationServer;
	}

	/**
	 * @return boolean indicating if the node is initial loaded.
	 */
	public boolean getInitialLoaded() {
		return initialLoaded;
	}

	/**
	 * @param isInitialLoaded boolean indicating if the node is initial loaded.
	 */
	public void setIsInitialLoaded(boolean isInitialLoaded) {
		this.initialLoaded = isInitialLoaded;
	}

	/**
	 * @return The node's ID.
	 */
	public String getNodeId() {
		return nodeId;
	}

	/**
	 * @param nodeId The node's ID.
	 */
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * @return The node's group id.
	 */
	public String getNodeGroupId() {
		return nodeGroupId;
	}

	/**
	 * @param nodeGroupId The node's group id.
	 */
	public void setNodeGroupId(String nodeGroupId) {
		this.nodeGroupId = nodeGroupId;
	}

	/**
	 * @return The node's external id.
	 */
	public String getExternalId() {
		return externalId;
	}

	/**
	 * @param externalId The node's external id.
	 */
	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}

	/**
	 * @return The sync URL other nodes would use to communicate with this node.
	 */
	public String getSyncUrl() {
		return syncUrl;
	}

	/**
	 * @param syncUrl The sync URL other nodes would use to communicate with this node.
	 */
	public void setSyncUrl(String syncUrl) {
		this.syncUrl = syncUrl;
	}

	/**
	 * @return The database type the node connects to.
	 */
	public String getDatabaseType() {
		return databaseType;
	}

	/**
	 * @param databaseType The database type the node connects to.
	 */
	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	/**
	 * @return The version of SymmetricDS running on the node.
	 */
	public String getSymmetricVersion() {
		return symmetricVersion;
	}

	/**
	 * @param symmetricVersion The version of SymmetricDS running on the node.
	 */
	public void setSymmetricVersion(String symmetricVersion) {
		this.symmetricVersion = symmetricVersion;
	}

	/**
	 * @return The version of the database the node is connected to.
	 */
	public String getDatabaseVersion() {
		return databaseVersion;
	}

	/**
	 * @param databaseVersion The version of the database the node is connected to.
	 */
	public void setDatabaseVersion(String databaseVersion) {
		this.databaseVersion = databaseVersion;
	}

	/**
	 * @return boolean indicating if synchronization is enabled on the node.
	 */
	public boolean isSyncEnabled() {
		return syncEnabled;
	}

	/**
	 * @param syncEnabled boolean indicating if synchronization is enabled on the node.
	 */
	public void setSyncEnabled(boolean syncEnabled) {
		this.syncEnabled = syncEnabled;
	}

	/**
	 * @return Node ID of the node that created this node.
	 */
	public String getCreatedAtNodeId() {
		return createdAtNodeId;
	}

	/**
	 * @param createdAtNodeId Node ID of the node that created this node.
	 */
	public void setCreatedAtNodeId(String createdAtNodeId) {
		this.createdAtNodeId = createdAtNodeId;
	}

	/**
	 * @return Number of batches waiting to be sent.
	 */
	public int getBatchToSendCount() {
		return batchToSendCount;
	}

	/**
	 * @param batchToSendCount Number of batches waiting to be sent.
	 */
	public void setBatchToSendCount(int batchToSendCount) {
		this.batchToSendCount = batchToSendCount;
	}

	/**
	 * @return Number of batches in the error state.
	 */
	public int getBatchInErrorCount() {
		return batchInErrorCount;
	}

	/**
	 * @param batchInErrorCount Number of batches in the error state.
	 */
	public void setBatchInErrorCount(int batchInErrorCount) {
		this.batchInErrorCount = batchInErrorCount;
	}

	/**
	 * @return The node's SymmetricDS installation type. (e.g., 'professional')
	 */
	public String getDeploymentType() {
		return deploymentType;
	}

	/**
	 * @param deploymentType The node's SymmetricDS installation type. (e.g., 'professional')
	 */
	public void setDeploymentType(String deploymentType) {
		this.deploymentType = deploymentType;
	}

}
