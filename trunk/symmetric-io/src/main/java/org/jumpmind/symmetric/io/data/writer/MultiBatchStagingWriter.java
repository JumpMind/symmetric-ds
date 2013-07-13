package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.symmetric.io.stage.IStagingManager;

public class MultiBatchStagingWriter extends StagingDataWriter {

    public MultiBatchStagingWriter(String sourceNodeId, String category,
            IStagingManager stagingManager, long[] batchIds, long maxBatchSize) {
        super(sourceNodeId, category, stagingManager, (IProtocolDataWriterListener[])null);
    }

    // TODO
}
