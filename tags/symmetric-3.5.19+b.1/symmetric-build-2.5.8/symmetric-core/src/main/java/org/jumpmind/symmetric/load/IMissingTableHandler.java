package org.jumpmind.symmetric.load;


/**
 * If an {@link IDataLoaderFilter} implements this interface it may indicate whether it will
 * handle the synchronization for a missing table at the target database.
 */
public interface IMissingTableHandler {

    public boolean isHandlingMissingTable(IDataLoaderContext ctx);
    
}
