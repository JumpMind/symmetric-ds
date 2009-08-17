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

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

/**
 * An out of the box filter that checks to see if the SymmetricDS configuration
 * has changed. If it has, it will take the correct action to apply the
 * configuration change to the current node.
 */
public class ConfigurationChangedFilter implements IDataLoaderFilter, IBatchListener {

    static final ILog log = LogFactory.getLog(ConfigurationChangedFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = "Resync." + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels." + ConfigurationChangedFilter.class.getSimpleName()
            + hashCode();

    private IParameterService parameterService;

    private IConfigurationService configurationService;
    
    private ITriggerRouterService triggerRouterService;

    private String tablePrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        return true;
    }

    private void recordSyncNeeded(IDataLoaderContext context) {
        if (isSyncTriggersNeeded(context)) {
            context.getContextCache().put(CTX_KEY_RESYNC_NEEDED, true);
        }
    }

    private void recordChannelFlushNeeded(IDataLoaderContext context) {
        if (isChannelFlushNeeded(context)) {
            context.getContextCache().put(CTX_KEY_FLUSH_CHANNELS_NEEDED, true);
        }
    }

    private boolean isSyncTriggersNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_TRIGGER);
    }

    private boolean isChannelFlushNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_CHANNEL);
    }

    private boolean matchesTable(IDataLoaderContext context, String tableSuffix) {
        return context.getTableName().equalsIgnoreCase(TableConstants.getTableName(tablePrefix, tableSuffix));
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
    }

    public void batchRolledback(IDataLoader loader, IncomingBatch batch) {
    }

    public void batchCommitted(IDataLoader loader, IncomingBatch batch) {
        if (loader.getContext().getContextCache().get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
            log.info("ChannelFlushed");
            configurationService.reloadChannels();
        }
        if (loader.getContext().getContextCache().get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
            log.info("ConfigurationChanged");
            triggerRouterService.syncTriggers();
        }
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }

    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
