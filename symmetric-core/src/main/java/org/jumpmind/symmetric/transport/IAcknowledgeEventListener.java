package org.jumpmind.symmetric.transport;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.BatchAck;

/**
 * Listener for changes in the batch status.  This extension point 
 * is called at the point of extraction.
 */
public interface IAcknowledgeEventListener extends IExtensionPoint {

	/**
	 * Batch status change event handler. 
	 * @param batchInfo The batch metadata.
	 */
	public void onAcknowledgeEvent(BatchAck batchInfo);
	
}