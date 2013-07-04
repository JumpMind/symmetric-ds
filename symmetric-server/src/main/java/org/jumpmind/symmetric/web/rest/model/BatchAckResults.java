package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

import org.jumpmind.symmetric.model.BatchAckResult;

public class BatchAckResults {

	/**
	 * A list of batch ack results
	 */
	private List<BatchAckResult> batchAckResults;

	public List<BatchAckResult> getBatchAckResults() {
		return batchAckResults;
	}

	public void setBatchAckResults(List<BatchAckResult> batchAckResults) {
		this.batchAckResults = batchAckResults;
	}

}
