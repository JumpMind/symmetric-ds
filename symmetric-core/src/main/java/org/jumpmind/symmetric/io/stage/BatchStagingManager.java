package org.jumpmind.symmetric.io.stage;

import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_INCOMING;
import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_OUTGOING;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.BatchId;

public class BatchStagingManager extends StagingManager {

    ISymmetricEngine engine;

    public BatchStagingManager(ISymmetricEngine engine, String directory) {
        super(directory);
        this.engine = engine;
    }

    @Override
    public long clean(long ttlInMs) {
        boolean recordIncomingBatchesEnabled = engine.getIncomingBatchService().isRecordOkBatchesEnabled();
        List<Long> outgoingBatches = ttlInMs == 0 ? new ArrayList<Long>() : engine.getOutgoingBatchService().getAllBatches();
        List<BatchId> incomingBatches =  ttlInMs == 0 ? new ArrayList<BatchId>() :  engine.getIncomingBatchService().getAllBatches();
        synchronized (StagingManager.class) {
            log.trace("Purging staging area");
            Set<String> keys = new HashSet<String>(resourceList.keySet());
            long purgedFileCount = 0;
            long purgedFileSize = 0;
            for (String key : keys) {
                IStagedResource resource = resourceList.get(key);
                String[] path = key.split("/");
                /*
                 * resource could have deleted itself between the time the keys
                 * were cloned and now
                 */
                if (resource != null) {
                    boolean resourceIsOld = (System.currentTimeMillis() - resource.getLastUpdateTime()) > ttlInMs;
                    if (path[0].equals(STAGING_CATEGORY_OUTGOING)) {
                        try {
                            Long batchId = new Long(path[path.length - 1]);
                            if (!outgoingBatches.contains(batchId) || ttlInMs == 0) {
                                purgedFileCount++;
                                purgedFileSize+=resource.getSize();
                                resource.delete();
                            }
                        } catch (NumberFormatException e) {
                            if (resourceIsOld || ttlInMs == 0) {
                                purgedFileCount++;
                                purgedFileSize+=resource.getSize();
                                resource.delete();
                            }
                        }
                    } else if (path[0].equals(STAGING_CATEGORY_INCOMING)) {
                        try {
                            BatchId batchId = new BatchId(new Long(path[path.length - 1]), path[1]);
                            if ((recordIncomingBatchesEnabled && !incomingBatches.contains(batchId))
                                    || (!recordIncomingBatchesEnabled && resourceIsOld) || ttlInMs == 0) {
                                purgedFileCount++;
                                purgedFileSize+=resource.getSize();
                                resource.delete();
                            }
                        } catch (NumberFormatException e) {
                            if (resourceIsOld || ttlInMs == 0) {
                                purgedFileCount++;
                                purgedFileSize+=resource.getSize();
                                resource.delete();
                            }
                        }
                    }
                }
            }
            if (purgedFileCount > 0) {
                if (purgedFileSize < 1000) {
                    log.info("Purged {} from stage, freeing {} bytes of space", purgedFileCount, (int) (purgedFileSize));
                } else {
                    log.info("Purged {} from stage, freeing {} kbytes of space", purgedFileCount, (int) (purgedFileSize / 1000));
                }
            }
            return purgedFileCount;
        }
    }

}
