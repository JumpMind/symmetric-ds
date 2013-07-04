package org.jumpmind.symmetric.model;

public class BatchAckResult {
	
	private long batchId;
	private boolean isOk;

	public BatchAckResult(BatchAck result) {
		this.batchId = result.getBatchId();
		this.isOk = true;
	}
	
	public long getBatchId() {
		return batchId;
	}

	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}

	public boolean isOk() {
		return isOk;
	}
	public void setOk(boolean isOk) {
		this.isOk = isOk;
	}
	
}
