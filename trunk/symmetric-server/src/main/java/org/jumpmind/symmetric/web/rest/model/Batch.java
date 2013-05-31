package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

public class Batch {

	/**
	 * The batch id for the given batch
	 */
	private long batchId;
	
	/**
	 * The list of sql statements captured on the source for this batch
	 */
	private List<String> sqlStatements;
	
	/**
	 * Returns the batchId for this batch
	 * @return
	 */
	public long getBatchId() {
		return batchId;
	}
	
	/**
	 * Setter for the batch id field.
	 * @param batchId
	 */
	public void setBatchId(long batchId) {
		this.batchId = batchId;
	}
	
	/**
	 * Returns the list of sql statements captured in this batch
	 * @return
	 */
	public List<String> getSqlStatements() {
		return sqlStatements;
	}
	
	/**
	 * Setter for the sqlStatements field
	 * @param sqlStatements
	 */
	public void setSqlStatements(List<String> sqlStatements) {
		this.sqlStatements = sqlStatements;
	}
}
