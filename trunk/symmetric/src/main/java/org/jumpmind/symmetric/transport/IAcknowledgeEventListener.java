package org.jumpmind.symmetric.transport;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.BatchInfo;

/**
 * Listener for changes in the batch status.
 * @author jkrajewski
 */
public interface IAcknowledgeEventListener extends IExtensionPoint {

	/**
	 * Batch status change event handler. 
	 * @param batchInfo The batch metadata.
	 */
	public void onAcknowledgeEvent(BatchInfo batchInfo);
}
