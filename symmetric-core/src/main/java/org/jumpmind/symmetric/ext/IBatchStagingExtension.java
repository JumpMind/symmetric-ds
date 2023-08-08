package org.jumpmind.symmetric.ext;

import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.StagingPurgeContext;

public interface IBatchStagingExtension extends IExtensionPoint {
    public void beforeClean(StagingPurgeContext context);

    public boolean isValidPath(String category);

    public boolean shouldCleanPath(IStagedResource resource, long ttlInMs, StagingPurgeContext context, String[] path, boolean resourceIsOld,
            boolean resourceClearsMinTimeHurdle);
}
