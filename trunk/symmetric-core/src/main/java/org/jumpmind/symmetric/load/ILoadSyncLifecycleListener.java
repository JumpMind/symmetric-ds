package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.model.IncomingBatch;

/**
 * This extension point is called prior to and after the data loader does it's
 * work for a single client connection. Multiple batches can be loaded as part
 * of the connection.
 */
public interface ILoadSyncLifecycleListener extends IExtensionPoint {

    public void syncStarted(DataContext context);

    public void syncEnded(DataContext context, List<IncomingBatch> batchesProcessed, Throwable ex);

}
