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

package org.jumpmind.symmetric.db.hsqldb2;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 */
public class HsqlDb2Dialect extends AbstractDbDialect implements IDbDialect {

    @Override
    public void init(Platform pf, int queryTimeout, JdbcTemplate jdbcTemplate) {
        super.init(pf, queryTimeout, jdbcTemplate);
        jdbcTemplate.execute("SET DATABASE DEFAULT TABLE TYPE CACHED");
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName,
            String tableName, String triggerName) {
        boolean exists = (jdbcTemplate.queryForInt(
                "select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ?",
                new Object[] { triggerName }) > 0);
        return exists;
    }
    
    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName, TriggerHistory oldHistory) {
        final String dropSql = String.format("DROP TRIGGER %s", triggerName);
        logSql(dropSql, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                int count = jdbcTemplate.update(dropSql);
                if (count > 0) {
                    log.info("TriggerDropped", triggerName);
                }
            } catch (Exception e) {
                log.warn("TriggerDropError", triggerName, e.getMessage());
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

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('sync_prevented','1')");
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('node_value','" + nodeId + "')");
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('sync_prevented',null)");
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('node_value',null)");
    }

    public String getSyncTriggersExpression() {
        return " " + tablePrefix + "_get_session('sync_prevented') is null ";
    }

    /**
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        // TODO A method is coming that will all access to the transaction id ...
        return "null";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
    
    @Override
    public boolean isBlankCharColumnSpacePadded() {
        return true;
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
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return (String) jdbcTemplate.queryForObject("select value from INFORMATION_SCHEMA.SYSTEM_SESSIONINFO where key='CURRENT SCHEMA'",
                String.class);
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    @Override
    public boolean canGapsOccurInCapturedDataIds() {
        return false;
    }
    
}