package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IBootstrapService;

/**
 *   
 */
public class SyncTriggersRequiredFilter implements IDataLoaderFilter,
        IBatchListener {

    final String CTX_KEY_RESYNC_NEEDED = SyncTriggersRequiredFilter.class.getSimpleName() + hashCode();
    
    private IBootstrapService bootstrapService;
    
    private String tablePrefix;
    
    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        recordSyncNeeded(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context,
            String[] columnValues) {
        recordSyncNeeded(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context,
            String[] columnValues, String[] keyValues) {
        recordSyncNeeded(context);
        return true;
    }
    
    private void recordSyncNeeded(IDataLoaderContext context) {
        if (isSyncTriggersNeeded(context)) {
            context.getContextCache().put(CTX_KEY_RESYNC_NEEDED, true);
        }
    }
    
    private boolean isSyncTriggersNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_TRIGGER);
    }
    
    private boolean matchesTable(IDataLoaderContext context, String tableSuffix) {
        return context.getTableName().equalsIgnoreCase(String.format("%s_%s", tablePrefix, tableSuffix));
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void batchComplete(IDataLoader loader, IncomingBatchHistory history) {
        if (loader.getContext().getContextCache().get(CTX_KEY_RESYNC_NEEDED) != null) {
            bootstrapService.syncTriggers();
        }
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

}
