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


package org.jumpmind.symmetric.db.db2;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * A dialect that is specific to DB2 databases
 */
public class Db2DbDialect extends AbstractDbDialect implements IDbDialect {

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }
    
    protected boolean createTablesIfNecessary() {
        boolean tablesCreated = super.createTablesIfNecessary();
        if (tablesCreated) {
             long triggerHistId = jdbcTemplate.queryForLong("select max(trigger_hist_id) from " + tablePrefix + "_trigger_hist")+1;
             jdbcTemplate.update("alter table " + tablePrefix + "_trigger_hist alter column trigger_hist_id restart with " + triggerHistId);
             log.info("DB2ResettingAutoIncrementColumns", tablePrefix + "_trigger_hist");
             long outgoingBatchId = jdbcTemplate.queryForLong("select max(batch_id) from " + tablePrefix + "_outgoing_batch")+1;
             jdbcTemplate.update("alter table " + tablePrefix + "_outgoing_batch alter column batch_id restart with " + outgoingBatchId);
             log.info("DB2ResettingAutoIncrementColumns", tablePrefix + "_outgoing_batch");
             long dataId = jdbcTemplate.queryForLong("select max(data_id) from " + tablePrefix + "_data")+1;
             jdbcTemplate.update("alter table " + tablePrefix + "_data alter column data_id restart with " + dataId);
             log.info("DB2ResettingAutoIncrementColumns", tablePrefix + "_data");
        }
        return tablesCreated;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from syscat.triggers where trigname = ? and trigschema = ?",
                new Object[] { triggerName.toUpperCase(), schema.toUpperCase() }) > 0;
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

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
    }

    public String getSyncTriggersExpression() {
        return "1=1";
    }
    
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "null";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return true;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        String defaultSchema = super.getDefaultSchema();
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA", String.class);
        }
        return defaultSchema;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "";
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }
    
    @Override
    protected String getDbSpecificDataHasChangedCondition() {
        return "var_row_data != var_old_data";
    }
    
}