package org.jumpmind.symmetric.route;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * An extension point that can be configured for a channel to allow further control over batching algorithms.
 * <P>
 * This is the point where the decision is made whether to end a batch or not.
 * 
 * @since 2.0
 */
public interface IBatchAlgorithm extends IExtensionPoint {
    public boolean isBatchComplete(OutgoingBatch batch, DataMetaData dataMetaData,
            SimpleRouterContext routingContext);
}