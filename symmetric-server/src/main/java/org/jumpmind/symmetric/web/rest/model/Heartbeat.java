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

public class Heartbeat {

	private String nodeId; 
	private String hostName;
	private String ipAddress;
	private String osUser;	
	private String osName;
	private String osArchitecture;
	private String osVersion;
	private Integer availableProcessors;
	private Long freeMemoryBytes;
	private Long totalMemoryBytes;
	private Long maxMemoryBytes;	
	private String javaVersion;
	private String javaVendor;
	private String symmetricVersion;
	private String timezoneOffset;
	private Date heartbeatTime;
	private Date lastRestartTime;
	private Date createTime;
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getOsUser() {
		return osUser;
	}
	public void setOsUser(String osUser) {
		this.osUser = osUser;
	}
	public String getOsName() {
		return osName;
	}
	public void setOsName(String osName) {
		this.osName = osName;
	}
	public String getOsArchitecture() {
		return osArchitecture;
	}
	public void setOsArchitecture(String osArchitecture) {
		this.osArchitecture = osArchitecture;
	}
	public String getOsVersion() {
		return osVersion;
	}
	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}
	public Integer getAvailableProcessors() {
		return availableProcessors;
	}
	public void setAvailableProcessors(Integer availableProcessors) {
		this.availableProcessors = availableProcessors;
	}
	public Long getFreeMemoryBytes() {
		return freeMemoryBytes;
	}
	public void setFreeMemoryBytes(Long freeMemoryBytes) {
		this.freeMemoryBytes = freeMemoryBytes;
	}
	public Long getTotalMemoryBytes() {
		return totalMemoryBytes;
	}
	public void setTotalMemoryBytes(Long totalMemoryBytes) {
		this.totalMemoryBytes = totalMemoryBytes;
	}
	public Long getMaxMemoryBytes() {
		return maxMemoryBytes;
	}
	public void setMaxMemoryBytes(Long maxMemoryBytes) {
		this.maxMemoryBytes = maxMemoryBytes;
	}
	public String getJavaVersion() {
		return javaVersion;
	}
	public void setJavaVersion(String javaVersion) {
		this.javaVersion = javaVersion;
	}
	public String getJavaVendor() {
		return javaVendor;
	}
	public void setJavaVendor(String javaVendor) {
		this.javaVendor = javaVendor;
	}
	public String getSymmetricVersion() {
		return symmetricVersion;
	}
	public void setSymmetricVersion(String symmetricVersion) {
		this.symmetricVersion = symmetricVersion;
	}
	public String getTimezoneOffset() {
		return timezoneOffset;
	}
	public void setTimezoneOffset(String timezoneOffset) {
		this.timezoneOffset = timezoneOffset;
	}
	public Date getHeartbeatTime() {
		return heartbeatTime;
	}
	public void setHeartbeatTime(Date heartbeatTime) {
		this.heartbeatTime = heartbeatTime;
	}
	public Date getLastRestartTime() {
		return lastRestartTime;
	}
	public void setLastRestartTime(Date lastRestartTime) {
		this.lastRestartTime = lastRestartTime;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
}
