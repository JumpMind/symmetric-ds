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

package org.jumpmind.symmetric.db.h2;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractEmbeddedSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Synchronization support for the H2 database platform. 
 */
public class H2SymmetricDialect extends AbstractEmbeddedSymmetricDialect implements ISymmetricDialect {
    
    public H2SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new H2TriggerTemplate(this);
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName, String tableName,
            String triggerName) {
        boolean exists = (platform.getSqlTemplate()
                .queryForInt("select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ?",
                        new Object[] { triggerName }) > 0)
                && (platform.getSqlTemplate().queryForInt("select count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        new Object[] { String.format("%s_CONFIG", triggerName) }) > 0);

        if (!exists) {
            removeTrigger(new StringBuilder(), catalogName, schemaName, triggerName, tableName, null);
        }
        return exists;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        final String dropSql = String.format("DROP TRIGGER IF EXISTS %s", triggerName);
        logSql(dropSql, sqlBuffer);

        final String dropTable = String.format("DROP TABLE IF EXISTS %s_CONFIG", triggerName);
        logSql(dropTable, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                int count = platform.getSqlTemplate().update(dropSql);
                if (count > 0) {
                    log.info("Just dropped trigger {}", triggerName);
                }
                count = platform.getSqlTemplate().update(dropTable);
                if (count > 0) {
                    log.info("Just dropped table {}_CONFIG", triggerName);
                }
            } catch (Exception e) {
                log.warn("Error removing {}: {}", triggerName, e.getMessage());
            }
        }
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("set @sync_prevented=1");
        transaction.prepareAndExecute("set @node_value=?", nodeId);
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("set @sync_prevented=null");
        transaction.prepareAndExecute("set @node_value=null");
    }

    public String getSyncTriggersExpression() {
        return " @sync_prevented is null ";
    }

    /*
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "TRANSACTION_ID()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }


    @Override
    public boolean supportsTransactionId() {
        return true;
    }

}
