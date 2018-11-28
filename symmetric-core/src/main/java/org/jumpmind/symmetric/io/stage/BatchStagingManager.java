package org.jumpmind.symmetric.io.stage;

import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_INCOMING;

import static org.jumpmind.symmetric.common.Constants.STAGING_CATEGORY_OUTGOING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.model.BatchId;
import org.jumpmind.symmetric.service.ClusterConstants;

public class BatchStagingManager extends StagingManager {

    ISymmetricEngine engine;

    public BatchStagingManager(ISymmetricEngine engine, String directory) {
        super(directory,engine.getParameterService().is(ParameterConstants.CLUSTER_LOCKING_ENABLED));
        this.engine = engine;
    }
    
    protected Map<String, Long> getBiggestBatchIds(Set<BatchId> batches) {
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
        boolean isLockAcquired = false;
        try {
            boolean clusterStagingEnabled = engine.getParameterService().is(ParameterConstants.CLUSTER_STAGING_ENABLED, false);
            if (clusterStagingEnabled) {
                if (engine.getClusterService().lock(ClusterConstants.STAGE_MANAGEMENT)) {
                    isLockAcquired = true;
                } else {
                    log.debug("Could not get a lock to run stage management");
                    return 0;
                }
            }
        } catch (Exception e) {
            // during setup or un-install, it's possible sym_lock table isn't available yet
        }
        try {
            boolean purgeBasedOnTTL = engine.getParameterService().is(ParameterConstants.STREAM_TO_FILE_PURGE_ON_TTL_ENABLED, false);
            boolean recordIncomingBatchesEnabled = engine.getIncomingBatchService().isRecordOkBatchesEnabled();
            long minTtlInMs = engine.getParameterService().getLong(ParameterConstants.STREAM_TO_FILE_MIN_TIME_TO_LIVE_MS,600000);
            Set<Long> outgoingBatches = ttlInMs == 0 ? new HashSet<Long>() : new HashSet<Long>(engine.getOutgoingBatchService().getAllBatches());
            Set<BatchId> incomingBatches =  ttlInMs == 0 ? new HashSet<BatchId>() : new HashSet<BatchId>(engine.getIncomingBatchService().getAllBatches());
            Map<String, Long> biggestIncomingByNode = getBiggestBatchIds(incomingBatches);
            
            StagingPurgeContext context = new StagingPurgeContext();
            context.putContextValue("purgeBasedOnTTL", purgeBasedOnTTL);
            context.putContextValue("recordIncomingBatchesEnabled", recordIncomingBatchesEnabled);
            context.putContextValue("minTtlInMs", minTtlInMs);
            context.putContextValue("outgoingBatches", outgoingBatches);
            context.putContextValue("incomingBatches", incomingBatches);
            context.putContextValue("biggestIncomingByNode", biggestIncomingByNode);
            
            return super.clean(ttlInMs, context);            
        } finally {            
            if (isLockAcquired) {
                try {
                    engine.getClusterService().unlock(ClusterConstants.STAGE_MANAGEMENT);
                } catch (Exception e) {
                }
            }
        }
    }
    
    @Override
    protected boolean shouldCleanPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context) {
        if (context.getBoolean("purgeBasedOnTTL")) {
            return super.shouldCleanPath(resource, ttlInMs, context);
        }
        
        if (resource.isInUse() || resource.getState() != State.DONE) {
            return false;
        }
        
        String[] path = resource.getPath().split("/");
        
        boolean resourceIsOld = (System.currentTimeMillis() - resource.getLastUpdateTime()) > ttlInMs;
        boolean resourceClearsMinTimeHurdle = (System.currentTimeMillis() - resource.getLastUpdateTime()) > context.getLong("minTtlInMs");
        
        if (path[0].equals(STAGING_CATEGORY_OUTGOING)) {
            return shouldCleanOutgoingPath(resource, ttlInMs, context, path, resourceIsOld);
        }  else if (path[0].equals(STAGING_CATEGORY_INCOMING)) {
            return shouldCleanIncomingPath(resource, ttlInMs, context, path, resourceIsOld, resourceClearsMinTimeHurdle);
        } else {
            log.warn("Unrecognized path: " + resource.getPath());
        }        
        return false;
    }
    
    protected boolean shouldCleanOutgoingPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context, String[] path,
            boolean resourceIsOld) {
        Set<Long> outgoingBatches = (Set<Long>) context.getContextValue("outgoingBatches");
        try {
            Long batchId = new Long(path[path.length - 1]);
            if (!outgoingBatches.contains(batchId) || ttlInMs == 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            if (resourceIsOld || ttlInMs == 0) {
                return true;
            }
        }
        
        return false;
    }
    
    protected boolean shouldCleanIncomingPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context, String[] path, boolean resourceIsOld,
            boolean resourceClearsMinTimeHurdle) {
        Set<BatchId> incomingBatches = (Set<BatchId>) context.getContextValue("incomingBatches");
        Map<String, Long> biggestIncomingByNode = (Map<String, Long>) context.getContextValue("biggestIncomingByNode");
        boolean recordIncomingBatchesEnabled = context.getBoolean("recordIncomingBatchesEnabled");
        try {
            BatchId batchId = new BatchId(new Long(path[path.length - 1]), path[1]);
            Long biggestBatchId = biggestIncomingByNode.get(batchId.getNodeId());
            if ((recordIncomingBatchesEnabled && !incomingBatches.contains(batchId) && 
                    biggestBatchId != null && biggestBatchId > batchId.getBatchId() &&
                    resourceClearsMinTimeHurdle)
                    || (!recordIncomingBatchesEnabled && resourceIsOld) || ttlInMs == 0) {
                return true;
            }
        } catch (NumberFormatException e) {
            if (resourceIsOld || ttlInMs == 0) {
                return true;
            }
        }
        
        return false;
    }

}
