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
