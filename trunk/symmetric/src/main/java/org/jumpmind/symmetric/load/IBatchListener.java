package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public interface IBatchListener extends IExtensionPoint {

    /**
     * This method is called after a batch has been successfully processed. It
     * is called in the scope of the transaction that controls the batch commit.
     */
    public void batchComplete(IDataLoader loader, IncomingBatchHistory history);
}
