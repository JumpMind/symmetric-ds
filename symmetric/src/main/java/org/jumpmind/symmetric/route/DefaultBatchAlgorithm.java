package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;

public class DefaultBatchAlgorithm implements IBatchAlgorithm {

    public boolean completeBatch(NodeChannel channel, OutgoingBatchHistory history, OutgoingBatch batch, Data data,
            boolean databaseTransactionBoundary) {        
        return history.getDataEventCount() >= channel.getMaxBatchSize() && databaseTransactionBoundary;
    }

    public boolean isAutoRegister() {
        return true;
    }

}
