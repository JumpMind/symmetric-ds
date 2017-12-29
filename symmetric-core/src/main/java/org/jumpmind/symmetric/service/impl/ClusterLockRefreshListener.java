package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataProcessorListener;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;

class ClusterLockRefreshListener implements IDataProcessorListener {
    
    protected IClusterService clusterService;
    
    public ClusterLockRefreshListener(IClusterService clusterService) {
        this.clusterService = clusterService;
    }
    
    @Override
    public boolean beforeBatchStarted(DataContext context) {
        return true;
    }

    @Override
    public void afterBatchStarted(DataContext context) {
    }

    @Override
    public void beforeBatchEnd(DataContext context) {
    }

    @Override
    public void batchSuccessful(DataContext context) {
    }

    @Override
    public void batchInError(DataContext context, Throwable ex) {
    }

    @Override
    public void batchProgressUpdate(DataContext context) {
        clusterService.refreshLock(ClusterConstants.INITIAL_LOAD_EXTRACT);
    }
    
}