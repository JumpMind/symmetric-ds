package org.jumpmind.symmetric.web.rest.model;

import java.util.List;

public class PullDataResults {

	/**
	 * The number of batches that were returned in this pull request
	 */
	private int nbrBatches;
	
	/**
	 * The actual list of {@link Batch}
	 */
	private List<Batch> batches;

	/**
	 * Returns the number of batches that were returned for this pull request
	 * @return
	 */
	public int getNbrBatches() {
		return nbrBatches;
	}

	/**
	 * Setter for the nbr batches field
	 * @param nbrBatches
	 */
	public void setNbrBatches(int nbrBatches) {
		this.nbrBatches = nbrBatches;
	}

	/**
	 * Gets the list of batches for this pull request
	 * @return
	 */
	public List<Batch> getBatches() {
		return batches;
	}

	/**
	 * Setter for the list of batches field
	 * @param batches
	 */
	public void setBatches(List<Batch> batches) {
		this.batches = batches;
	}

}
