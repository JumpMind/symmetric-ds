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
package org.jumpmind.symmetric.db.h2;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractEmbeddedSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Synchronization support for the H2 database platform.
 */
public class H2SymmetricDialect extends AbstractEmbeddedSymmetricDialect implements ISymmetricDialect {
    static final String SQL_DROP_FUNCTION = "DROP ALIAS $(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from INFORMATION_SCHEMA.FUNCTION_ALIASES where ALIAS_NAME='$(functionName)'";

    public H2SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new H2TriggerTemplate(this);
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName, String tableName,
            String triggerName) {
        boolean exists = (platform.getSqlTemplate()
                .queryForInt(
                        "select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ? and (TRIGGER_CATALOG=? or ? is null) and (TRIGGER_SCHEMA=? or ? is null)",
                        new Object[] { triggerName, catalogName, catalogName, schemaName, schemaName }) > 0)
                && (platform.getSqlTemplate().queryForInt(
                        "select count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? and (TABLE_CATALOG=? or ? is null) and (TABLE_SCHEMA=? or ? is null)",
                        new Object[] { String.format("%s_CONFIG", triggerName), catalogName, catalogName, schemaName, schemaName }) > 0);
        if (!exists && !StringUtils.isBlank(triggerName)) {
            removeTrigger(new StringBuilder(), catalogName, schemaName, triggerName, tableName);
        }
        return exists;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, ISqlTransaction transaction) {
        DatabaseInfo dbInfo = getPlatform().getDatabaseInfo();
        String prefix = Table.getFullyQualifiedTablePrefix(catalogName, schemaName, dbInfo.getDelimiterToken(),
                dbInfo.getCatalogSeparator(), dbInfo.getSchemaSeparator());
        final String dropSql = String.format("DROP TRIGGER IF EXISTS %s%s", prefix, triggerName);
        logSql(dropSql, sqlBuffer);
        final String dropTable = String.format("DROP TABLE IF EXISTS %s%s_CONFIG", prefix, triggerName);
        logSql(dropTable, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("Dropping trigger {} for {}", triggerName, Table.getFullyQualifiedTableName(catalogName, schemaName, tableName));
            transaction.execute(dropSql);
            transaction.execute(dropTable);
        }
    }

    @Override
    public void createRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix().toUpperCase() + "_" + "BASE64_ENCODE";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "CREATE ALIAS IF NOT EXISTS $(functionName) for \"org.jumpmind.symmetric.db.EmbeddedDbFunctions.encodeBase64\"; ";
            install(sql, encode);
        }
    }

    @Override
    public void dropRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix().toUpperCase() + "_" + "BASE64_ENCODE";
        if (installed(SQL_FUNCTION_INSTALLED, encode)) {
            uninstall(SQL_DROP_FUNCTION, encode);
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
        return H2Trigger.TRANSACTION_FUNCTION;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public void cleanupTriggers() {
        List<String> names = platform.getSqlTemplate().query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = SCHEMA() AND TABLE_NAME LIKE '" + parameterService.getTablePrefix()
                        .toUpperCase() + "_%_CONFIG'", new StringMapper());
        for (String name : names) {
            platform.getSqlTemplate().update("drop table " + name);
            log.info("Dropped table {}", name);
        }
    }
}
