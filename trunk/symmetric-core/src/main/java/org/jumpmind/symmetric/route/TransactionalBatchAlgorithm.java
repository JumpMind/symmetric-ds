package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * Only put individual transactions in a batch.
 */
public class TransactionalBatchAlgorithm implements IBatchAlgorithm {

    public boolean isBatchComplete(OutgoingBatch batch, DataMetaData dataMetaData,
            SimpleRouterContext routingContext) {
        return routingContext.isEncountedTransactionBoundary();
    }

}