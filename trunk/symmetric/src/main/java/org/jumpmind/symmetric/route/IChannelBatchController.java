package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;

/**
 * A possible extension point that could be configured by channels to allow further control over batching algorithms.  I am thinking that
 * we provide two implementations that can be configured at the channel level:  our default batching based on the number of events and another
 * implementation that would batch only on transaction boundaries.
 * @since 2.0
 */
public interface IChannelBatchController extends IExtensionPoint {  
    public boolean completeBatch(OutgoingBatchHistory history, OutgoingBatch batch, Data data, boolean databaseTransactionBoundary);
}
