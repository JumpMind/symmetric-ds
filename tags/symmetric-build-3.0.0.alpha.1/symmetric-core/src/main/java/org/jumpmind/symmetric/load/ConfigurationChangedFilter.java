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
 * under the License. 
 */

package org.jumpmind.symmetric.load;

import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An out of the box filter that checks to see if the SymmetricDS configuration
 * has changed. If it has, it will take the correct action to apply the
 * configuration change to the current node.
 */
public class ConfigurationChangedFilter extends DatabaseWriterFilterAdapter implements
        IBuiltInExtensionPoint {

    static final Logger log = LoggerFactory.getLogger(ConfigurationChangedFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedFilter.class.getSimpleName() + hashCode();

    private IParameterService parameterService;

    private IConfigurationService configurationService;

    private ITriggerRouterService triggerRouterService;

    private ITransformService transformService;

    public ConfigurationChangedFilter(IParameterService parameterService,
            IConfigurationService configurationService, ITriggerRouterService triggerRouterService,
            ITransformService transformService) {
        this.parameterService = parameterService;
        this.configurationService = configurationService;
        this.triggerRouterService = triggerRouterService;
        this.transformService = transformService;
    }

    @Override
    public void afterWrite(
            DataContext context, Table table, CsvData data) {
        recordSyncNeeded(context, table);
        recordChannelFlushNeeded(context, table);
        recordTransformFlushNeeded(context, table);
    }

    private void recordSyncNeeded(
            DataContext context, Table table) {
        if (isSyncTriggersNeeded(table)) {
            context.put(CTX_KEY_RESYNC_NEEDED, true);
        }
    }

    private void recordChannelFlushNeeded(
            DataContext context, Table table) {
        if (isChannelFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_CHANNELS_NEEDED, true);
        }
    }

    private <R extends IDataReader, W extends IDataWriter> void recordTransformFlushNeeded(
            DataContext context, Table table) {
        if (isTransformFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, true);
        }
    }

    private boolean isSyncTriggersNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_TRIGGER)
                || matchesTable(table, TableConstants.SYM_ROUTER)
                || matchesTable(table, TableConstants.SYM_TRIGGER_ROUTER)
                || matchesTable(table, TableConstants.SYM_NODE_GROUP_LINK);
    }

    private boolean isChannelFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_CHANNEL);
    }

    private boolean isTransformFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_TRANSFORM_COLUMN)
                || matchesTable(table, TableConstants.SYM_TRANSFORM_TABLE);
    }

    private boolean matchesTable(Table table, String tableSuffix) {
        if (table != null && table.getName() != null) {
            return table.getName().equalsIgnoreCase(
                    TableConstants.getTableName(parameterService.getTablePrefix(), tableSuffix));
        } else {
            return false;
        }
    }

    @Override
    public <R extends IDataReader, W extends IDataWriter> void batchCommitted(
            DataContext context) {
        if (context.get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
            log.info("Channels flushed because new channels came through the data loader");
            configurationService.reloadChannels();
        }
        if (context.get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("About to syncTriggers because new configuration came through the data loader");
            triggerRouterService.syncTriggers();
        }
        if (context.get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION)) {
            log.info("About to refresh the cache of transformation because new configuration came through the data loader");
            transformService.resetCache();
        }
    }

}
