package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * Batch algorithm that puts multiple transactions in the same batch up to the max
 * batch size, but never breaks a transaction boundary
 */
public class DefaultBatchAlgorithm implements IBatchAlgorithm {

    public boolean isBatchComplete(OutgoingBatch batch, DataMetaData dataMetaData, SimpleRouterContext routingContext) {
        return batch.getDataEventCount() >= dataMetaData.getNodeChannel().getMaxBatchSize()
                && routingContext.isEncountedTransactionBoundary();
    }

}