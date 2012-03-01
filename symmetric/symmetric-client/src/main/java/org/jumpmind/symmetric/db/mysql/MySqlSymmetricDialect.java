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

package org.jumpmind.symmetric.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

public class MySqlSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    private static final String TRANSACTION_ID = "transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";

    private String functionTemplateKeySuffix = null;

    public MySqlSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerText = new MySqlTriggerTemplate(this);
        this.parameterService = parameterService;
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        int[] versions = Version.parseVersion(getProductVersion());
        if (getMajorVersion() == 5
                && (getMinorVersion() == 0 || (getMinorVersion() == 1 && versions[2] < 23))) {
            this.functionTemplateKeySuffix = "_pre_5_1_23";
        } else {
            this.functionTemplateKeySuffix = "_post_5_1_23";
        }
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    protected void createRequiredFunctions() {
        String[] functions = triggerText.getFunctionsToInstall();
        for (int i = 0; i < functions.length; i++) {
            if (functions[i].endsWith(this.functionTemplateKeySuffix)) {
                String funcName = parameterService.getTablePrefix()
                        + "_"
                        + functions[i].substring(0, functions[i].length()
                                - this.functionTemplateKeySuffix.length());
                if (platform.getSqlTemplate().queryForInt(
                        triggerText.getFunctionInstalledSql(funcName, platform.getDefaultSchema())) == 0) {
                    platform.getSqlTemplate().update(
                            triggerText.getFunctionSql(functions[i], funcName));
                    log.info("Just installed {}", funcName);
                }
            }
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        catalog = catalog == null ? (platform.getDefaultCatalog() == null ? null : platform
                .getDefaultCatalog()) : catalog;
        String checkCatalogSql = (catalog != null && catalog.length() > 0) ? " and trigger_schema='"
                + catalog + "'"
                : "";
        return platform
                .getSqlTemplate()
                .queryForInt(
                        "select count(*) from information_schema.triggers where trigger_name like ? and event_object_table like ?"
                                + checkCatalogSql, new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName, TriggerHistory oldHistory) {
        catalogName = catalogName == null ? "" : (catalogName + ".");
        final String sql = "drop trigger " + catalogName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                platform.getSqlTemplate().update(sql);
            } catch (Exception e) {
                log.warn("Trigger does not exist");
            }
        }
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            transaction
                    .prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }

    private final String getTransactionFunctionName() {
        return platform.getDefaultCatalog() + "." + parameterService.getTablePrefix() + "_"
                + TRANSACTION_ID;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return getTransactionFunctionName() + "()";
    }

    public void purge() {
    }

    @Override
    protected String switchCatalogForTriggerInstall(String catalog, ISqlTransaction transaction) {
        if (catalog != null) {
            Connection c = ((JdbcSqlTransaction) transaction).getConnection();
            String previousCatalog;
            try {
                previousCatalog = c.getCatalog();
                c.setCatalog(catalog);
                return previousCatalog;
            } catch (SQLException e) {
                throw new SqlException(e);
            }
        } else {
            return null;
        }
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_row_data != var_old_data";
    }
}
