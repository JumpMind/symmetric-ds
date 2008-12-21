/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.load;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * An out of the box filter that checks to see if the SymmetricDS trigger
 * configuration has changed. If it has, it will synchronize triggers.
 */
public class SyncTriggersRequiredFilter implements IDataLoaderFilter, IBatchListener {

    static final Log logger = LogFactory.getLog(SyncTriggersRequiredFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = SyncTriggersRequiredFilter.class.getSimpleName() + hashCode();

    private IBootstrapService bootstrapService;

    private IParameterService parameterService;

    private String tablePrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        recordSyncNeeded(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        recordSyncNeeded(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
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
        if (loader.getContext().getContextCache().get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
            logger.info("About to syncTriggers because new configuration came through the dataloader.");
            bootstrapService.syncTriggers();
        }
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

}
