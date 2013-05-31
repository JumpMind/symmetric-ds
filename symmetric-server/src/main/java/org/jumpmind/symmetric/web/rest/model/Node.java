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
package org.jumpmind.symmetric.web.rest.model;

import java.util.Date;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Node {

    private String nodeId;
    private String externalId;
	private boolean registrationServer;
    private String syncUrl;
    private String registrationUrl;
    private int batchToSendCount;    
    private int batchInErrorCount;
    private Date lastHeartbeat;
    private int heartbeatInterval;
	private boolean registered;
    private boolean initialLoaded;
    private boolean reverseInitialLoaded;
    private String createdAtNodeId;
    
    public String getCreatedAtNodeId() {
		return createdAtNodeId;
	}

	public void setCreatedAtNodeId(String createdAtNodeId) {
		this.createdAtNodeId = createdAtNodeId;
	}

	public boolean isReverseInitialLoaded() {
		return reverseInitialLoaded;
	}

	public void setReverseInitialLoaded(boolean reverseInitialLoaded) {
		this.reverseInitialLoaded = reverseInitialLoaded;
	}

	public int getBatchToSendCount() {
		return batchToSendCount;
	}

	public void setBatchToSendCount(int batchToSendCount) {
		this.batchToSendCount = batchToSendCount;
	}

	public int getBatchInErrorCount() {
		return batchInErrorCount;
	}

	public void setBatchInErrorCount(int batchInErrorCount) {
		this.batchInErrorCount = batchInErrorCount;
	}

	public boolean isRegistered() {
		return registered;
	}

	public void setRegistered(boolean registered) {
		this.registered = registered;
	}

	public boolean isInitialLoaded() {
		return initialLoaded;
	}

	public void setInitialLoaded(boolean initialLoaded) {
		this.initialLoaded = initialLoaded;
	}

	public String getSyncUrl() {
		return syncUrl;
	}

	public void setSyncUrl(String syncUrl) {
		this.syncUrl = syncUrl;
	}

	public boolean isRegistrationServer() {
		return registrationServer;
	}

	public void setRegistrationServer(boolean registrationServer) {
		this.registrationServer = registrationServer;
	}

	public Node(String nodeId) {
        setNodeId(nodeId);
    }

    public Node() {

    }

    public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public Date getLastHeartbeat() {
		return lastHeartbeat;
	}

	public void setLastHeartbeat(Date lastHeartbeat) {
		this.lastHeartbeat = lastHeartbeat;
	}
	
    public String getExternalId() {
		return externalId;
	}

	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}

	public String getRegistrationUrl() {
		return registrationUrl;
	}

	public void setRegistrationUrl(String registrationUrl) {
		this.registrationUrl = registrationUrl;
	}

	public int getHeartbeatInterval() {
		return heartbeatInterval;
	}

	public void setHeartbeatInterval(int heartbeatInterval) {
		this.heartbeatInterval = heartbeatInterval;
	}	
		
}
