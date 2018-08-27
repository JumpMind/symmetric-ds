package org.jumpmind.symmetric.io.stage;

import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_INCOMING;
import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_OUTGOING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.BatchId;
import org.jumpmind.symmetric.service.ClusterConstants;

public class BatchStagingManager extends StagingManager {

    ISymmetricEngine engine;

    public BatchStagingManager(ISymmetricEngine engine, String directory) {
        super(directory,engine.getParameterService().is(ParameterConstants.CLUSTER_LOCKING_ENABLED));
        this.engine = engine;
    }
    
    protected Map<String, Long> getBiggestBatchIds(List<BatchId> batches) {
        Map<String,Long> biggest = new HashMap<String,Long>();
        for (BatchId batchId : batches) {
            Long batchNumber = biggest.get(batchId.getNodeId());
            if (batchNumber == null || batchNumber < batchId.getBatchId()) {
                biggest.put(batchId.getNodeId(), batchId.getBatchId());
            }
        }        
        return biggest;
    }

    @Override
    public long clean(long ttlInMs) {
        try {
            boolean clusterStagingEnabled = engine.getParameterService().is(ParameterConstants.CLUSTER_STAGING_ENABLED, false);
            if (clusterStagingEnabled && !engine.getClusterService().lock(ClusterConstants.STAGE_MANAGEMENT)) {
                log.debug("Could not get a lock to run stage management");
                return 0;
            }
        } catch (Exception e) {
            // during setup or un-install, it's possible sym_lock table isn't available yet
        }

        try {            
            boolean purgeBasedOnTTL = engine.getParameterService().is(ParameterConstants.STREAM_TO_FILE_PURGE_ON_TTL_ENABLED, false);
            if (purgeBasedOnTTL) {
                return super.clean(ttlInMs);
            } else {
                synchronized (StagingManager.class) {
                    return purgeStagingBasedOnDatabaseStatus(ttlInMs);
                }
            }
        } finally {
            try {
                engine.getClusterService().unlock(ClusterConstants.STAGE_MANAGEMENT);
            } catch (Exception e) {
            }
        }
    }
    
    protected long purgeStagingBasedOnDatabaseStatus(long ttlInMs) {
        boolean recordIncomingBatchesEnabled = engine.getIncomingBatchService().isRecordOkBatchesEnabled();
        long minTtlInMs = engine.getParameterService().getLong(ParameterConstants.STREAM_TO_FILE_MIN_TIME_TO_LIVE_MS,600000);
        List<Long> outgoingBatches = ttlInMs == 0 ? new ArrayList<Long>() : engine.getOutgoingBatchService().getAllBatches();
        List<BatchId> incomingBatches =  ttlInMs == 0 ? new ArrayList<BatchId>() :  engine.getIncomingBatchService().getAllBatches();
        Map<String, Long> biggestIncomingByNode = getBiggestBatchIds(incomingBatches);
        synchronized (StagingManager.class) {
            log.trace("Purging staging area");
            Set<String> keys = getResourceReferences();
            long purgedFileCount = 0;
            long purgedFileSize = 0;
            for (String key : keys) {
                IStagedResource resource = find(key);
                String[] path = key.split("/");
                /*
                 * resource could have deleted itself between the time the keys
                 * were cloned and now
                 */
                if (resource != null && !resource.isInUse()) {
                    boolean resourceIsOld = (System.currentTimeMillis() - resource.getLastUpdateTime()) > ttlInMs;
                    boolean resourceClearsMinTimeHurdle = (System.currentTimeMillis() - resource.getLastUpdateTime()) > minTtlInMs;
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
                            Long biggestBatchId = biggestIncomingByNode.get(batchId.getNodeId());
                            if ((recordIncomingBatchesEnabled && !incomingBatches.contains(batchId) && 
                                    biggestBatchId != null && biggestBatchId > batchId.getBatchId() &&
                                    resourceClearsMinTimeHurdle)
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
