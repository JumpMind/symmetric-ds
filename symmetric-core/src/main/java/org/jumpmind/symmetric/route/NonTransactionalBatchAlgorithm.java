package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * Puts multiple transactions in a batch and ends exactly on the max batch size
 * if there is more than the max batch size of data to batch.  Breaks transactional
 * boundaries.
 */
public class NonTransactionalBatchAlgorithm implements IBatchAlgorithm {

    public boolean isBatchComplete(OutgoingBatch batch, DataMetaData dataMetaData, SimpleRouterContext routingContext) {
        return batch.getDataEventCount() >= dataMetaData.getNodeChannel().getMaxBatchSize();
    }
    
}