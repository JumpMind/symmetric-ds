/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.db;

import java.io.IOException;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.firebird.FirebirdDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.ext.IDatabaseUpgradeListener;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseUpgradeListener implements IDatabaseUpgradeListener, ISymmetricEngineAware {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ISymmetricEngine engine;

    protected boolean isUpgradeTo38;
    
    @Override
    public String beforeUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database currentModel,
            Database desiredModel) throws IOException {
        StringBuilder sb = new StringBuilder();
        String monitorTableName = tablePrefix + "_" + TableConstants.SYM_MONITOR;
        if (currentModel.findTable(monitorTableName) == null && desiredModel.findTable(monitorTableName) != null) {
            log.info("Detected upgrade to version 3.8");
            isUpgradeTo38 = true;
        } else {
            isUpgradeTo38 = false;
        }
        if (isUpgradeTo38) {
            Table transformTable = currentModel.findTable(tablePrefix + "_" + TableConstants.SYM_TRANSFORM_TABLE);
            if (transformTable != null && transformTable.findColumn("update_action") != null) {
                engine.getSqlTemplate().update("update " + tablePrefix + "_" + TableConstants.SYM_TRANSFORM_TABLE +
                        " set update_action = 'UPD_ROW' where update_action is null");
            }
        }
        
        if (engine.getDatabasePlatform() instanceof FirebirdDatabasePlatform) {
            String contextTableName = tablePrefix + "_" + TableConstants.SYM_CONTEXT;
            Table contextTable = currentModel.findTable(contextTableName);
            if (contextTable != null && contextTable.findColumn("value") != null) {
                TriggerHistory hist = engine.getTriggerRouterService().findTriggerHistory(null, null, contextTableName);
                if (hist != null) {
                    engine.getTriggerRouterService().dropTriggers(hist);
                }
            }

            String monitorEventTableName = tablePrefix + "_" + TableConstants.SYM_MONITOR_EVENT;
            Table monitorEventTable = currentModel.findTable(monitorEventTableName);
            if (monitorEventTable != null && monitorEventTable.findColumn("value") != null) {
                TriggerHistory hist = engine.getTriggerRouterService().findTriggerHistory(null, null, monitorEventTableName);
                if (hist != null) {
                    engine.getTriggerRouterService().dropTriggers(hist);
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String afterUpgrade(ISymmetricDialect symmetricDialect, String tablePrefix, Database model) throws IOException {
        StringBuilder sb = new StringBuilder();
        if (isUpgradeTo38) {
            engine.getSqlTemplate().update("update " + tablePrefix + "_" + TableConstants.SYM_SEQUENCE +
                    " set cache_size = 10 where sequence_name = ?", Constants.SEQUENCE_OUTGOING_BATCH);
            engine.getSqlTemplate().update("update  " + tablePrefix + "_" + TableConstants.SYM_CHANNEL +
            		" set max_batch_size = 10000 where reload_flag = 1 and max_batch_size = 10000");
        }
        return sb.toString();
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

}
