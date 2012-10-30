package org.jumpmind.symmetric.web.rest;

import org.jumpmind.symmetric.Version;

public class NodeStatus {

	Boolean registered;

	Boolean registrationServer;

	Boolean isInitialLoaded;

	private String nodeId;

	private String nodeGroupId;

	private String externalId;

	private String syncUrl;

	private String databaseType;

	private String databaseVersion;

	private boolean syncEnabled = true;

	private String createdAtNodeId;

	private int batchToSendCount;

	private int batchInErrorCount;

	private String deploymentType;
	
	private String symmetricVersion = Version.version();

	public Boolean getRegistered() {
		return registered;
	}

	public void setRegistered(Boolean registered) {
		this.registered = registered;
	}

	public Boolean getRegistrationServer() {
		return registrationServer;
	}

	public void setRegistrationServer(Boolean registrationServer) {
		this.registrationServer = registrationServer;
	}

	public Boolean getIsInitialLoaded() {
		return isInitialLoaded;
	}

	public void setIsInitialLoaded(Boolean isInitialLoaded) {
		this.isInitialLoaded = isInitialLoaded;
	}

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getNodeGroupId() {
		return nodeGroupId;
	}

	public void setNodeGroupId(String nodeGroupId) {
		this.nodeGroupId = nodeGroupId;
	}

	public String getExternalId() {
		return externalId;
	}

	public void setExternalId(String externalId) {
		this.externalId = externalId;
	}

	public String getSyncUrl() {
		return syncUrl;
	}

	public void setSyncUrl(String syncUrl) {
		this.syncUrl = syncUrl;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public String getSymmetricVersion() {
		return symmetricVersion;
	}

	public void setSymmetricVersion(String symmetricVersion) {
		this.symmetricVersion = symmetricVersion;
	}

	public String getDatabaseVersion() {
		return databaseVersion;
	}

	public void setDatabaseVersion(String databaseVersion) {
		this.databaseVersion = databaseVersion;
	}

	public boolean isSyncEnabled() {
		return syncEnabled;
	}

	public void setSyncEnabled(boolean syncEnabled) {
		this.syncEnabled = syncEnabled;
	}

	public String getCreatedAtNodeId() {
		return createdAtNodeId;
	}

	public void setCreatedAtNodeId(String createdAtNodeId) {
		this.createdAtNodeId = createdAtNodeId;
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

	public String getDeploymentType() {
		return deploymentType;
	}

	public void setDeploymentType(String deploymentType) {
		this.deploymentType = deploymentType;
	}

}
