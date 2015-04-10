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
package org.jumpmind.symmetric.db.db2;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * A dialect that is specific to DB2 databases
 */
public class Db2SymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public Db2SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new Db2TriggerTemplate(this);
    }

    public boolean createOrAlterTablesIfNecessary(String... tables) {
        boolean tablesCreated = super.createOrAlterTablesIfNecessary(tables);
        if (tablesCreated) {
            long dataId = platform.getSqlTemplate().queryForLong("select max(data_id) from " + parameterService.getTablePrefix()
                    + "_data") + 1;
            platform.getSqlTemplate().update("alter table " + parameterService.getTablePrefix()
                    + "_data alter column data_id restart with " + dataId);
            log.info("Resetting auto increment columns for {}", parameterService.getTablePrefix() + "_data");
        }
        return tablesCreated;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        schema = schema == null ? (platform.getDefaultSchema() == null ? null : platform
                .getDefaultSchema()) : schema;
        return platform.getSqlTemplate().queryForInt(
                "SELECT COUNT(*) FROM " + getSystemSchemaName() + ".SYSTRIGGERS WHERE NAME = ? AND SCHEMA = ?",
                new Object[] { triggerName.toUpperCase(), schema.toUpperCase() }) > 0;
    }
    
    @Override
    public String massageDataExtractionSql(String sql, Channel channel) {
        /* Remove tranaction_id from the sql because DB2 doesn't support transactions.  In fact,
         * DB2 iSeries does return results because the query asks for every column in the table PLUS
         * the router_id.  We max out the size of the table on iSeries so when you try to return the 
         * entire table + additional columns we go past the max size for a row
         */
        sql = sql.replace("d.transaction_id, ", "");
        return super.massageDataExtractionSql(sql, channel);
    }
    
    protected String getSystemSchemaName() {
    	return "SYSIBM";
    }

    @Override
    public void createRequiredDatabaseObjects() {        
    }
    
    @Override
    public void dropRequiredDatabaseObjects() {
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    }

    public String getSyncTriggersExpression() {
        return "1=1";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    public void cleanDatabase() {
    }

    @Override
    public void truncateTable(String tableName) {
        platform.getSqlTemplate().update("delete from " + tableName);
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_old_data is null or var_row_data != var_old_data";
    }

    @Override
    public String getSourceNodeExpression() {
        return "null";
    }
}
