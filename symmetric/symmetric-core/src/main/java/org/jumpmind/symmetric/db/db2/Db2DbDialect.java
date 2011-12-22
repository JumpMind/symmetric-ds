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

import org.jumpmind.db.BinaryEncoding;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

/*
 * A dialect that is specific to DB2 databases
 */
public class Db2DbDialect extends AbstractDbDialect implements IDbDialect {
    
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
        schema = schema == null ? (platform.getDefaultSchema() == null ? null : platform.getDefaultSchema()) : schema;
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


    public void enableSyncTriggers(ISqlTransaction transaction) {
    }
    
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
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

    @Override
    public boolean supportsTransactionId() {
        return false;
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

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }
    
    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_row_data != var_old_data";
    }
    
}