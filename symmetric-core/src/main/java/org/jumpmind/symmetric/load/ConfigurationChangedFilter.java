/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

package org.jumpmind.symmetric.load;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.transform.ITransformService;

/**
 * An out of the box filter that checks to see if the SymmetricDS configuration
 * has changed. If it has, it will take the correct action to apply the
 * configuration change to the current node.
 */
public class ConfigurationChangedFilter 
    implements IDataLoaderFilter, IBatchListener, IBuiltInExtensionPoint {

    static final ILog log = LogFactory.getLog(ConfigurationChangedFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = "Resync." + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels." + ConfigurationChangedFilter.class.getSimpleName()
            + hashCode();
    
    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms." + ConfigurationChangedFilter.class.getSimpleName()
    + hashCode();

    private IParameterService parameterService;

    private IConfigurationService configurationService;
    
    private ITriggerRouterService triggerRouterService;
    
    private ITransformService transformService;

    private String tablePrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        recordTransformFlushNeeded(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        recordTransformFlushNeeded(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        recordSyncNeeded(context);
        recordChannelFlushNeeded(context);
        recordTransformFlushNeeded(context);
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
    
    private void recordTransformFlushNeeded(IDataLoaderContext context) {
        if (isTransformFlushNeeded(context)) {
            context.getContextCache().put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, true);
        }
    }

    private boolean isSyncTriggersNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_TRIGGER) 
          || matchesTable(context, TableConstants.SYM_ROUTER) 
          || matchesTable(context, TableConstants.SYM_TRIGGER_ROUTER);
    }

    private boolean isChannelFlushNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_CHANNEL);
    }
    
    private boolean isTransformFlushNeeded(IDataLoaderContext context) {
        return matchesTable(context, TableConstants.SYM_TRANSFORM_COLUMN) || 
        matchesTable(context, TableConstants.SYM_TRANSFORM_TABLE);
    }

    private boolean matchesTable(IDataLoaderContext context, String tableSuffix) {
        if (context.getTableName() != null) {
            return context.getTableName().equalsIgnoreCase(TableConstants.getTableName(tablePrefix, tableSuffix));
        } else {
            return false;
        }
    }

    public boolean isAutoRegister() {
        return true;
    }
    
    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
    }
    
    public void batchRolledback(IDataLoader loader, IncomingBatch batch, Exception ex) {
    }
    
    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {};

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
        if (loader.getContext().getContextCache().get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
            log.info("ConfigurationChanged");
            transformService.resetCache();
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

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }
}