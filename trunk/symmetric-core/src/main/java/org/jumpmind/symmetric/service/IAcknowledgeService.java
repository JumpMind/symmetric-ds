
package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;

/**
 * This service provides an API to access acknowledge {@link OutgoingBatch}s.
 */
public interface IAcknowledgeService {

    public void ack(BatchAck batch);
    
    public void addAcknowledgeEventListener(IAcknowledgeEventListener statusChangeListner);

}