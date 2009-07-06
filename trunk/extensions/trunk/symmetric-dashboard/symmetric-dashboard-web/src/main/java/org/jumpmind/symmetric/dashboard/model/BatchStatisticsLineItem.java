package org.jumpmind.symmetric.dashboard.model;

/**
 * @author jkrajewski
 *
 */
public class BatchStatisticsLineItem {

	private String nodeId;
	private int totalBatches;
	private int totalSuccess;
	private int totalFailures;
	private int totalCreated;
	private float averageSuccessRate;
	
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public int getTotalBatches() {
		return totalBatches;
	}
	public void setTotalBatches(int totalBatches) {
		this.totalBatches = totalBatches;
	}
	public int getTotalSuccess() {
		return totalSuccess;
	}
	public void setTotalSuccess(int totalSuccess) {
		this.totalSuccess = totalSuccess;
	}
	public int getTotalFailures() {
		return totalFailures;
	}
	public void setTotalFailures(int totalFailures) {
		this.totalFailures = totalFailures;
	}
	public int getTotalCreated() {
		return totalCreated;
	}
	public void setTotalCreated(int totalCreated) {
		this.totalCreated = totalCreated;
	}
	public float getAverageSuccessRate() {
		return averageSuccessRate;
	}
	public void setAverageSuccessRate(float averageSuccessRate) {
		this.averageSuccessRate = averageSuccessRate;
	}
}
