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
package org.jumpmind.symmetric.db.firebird;

import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Database dialect for <a href="http://www.firebirdsql.org/">Firebird</a>.
 */
public class FirebirdDbDialect extends AbstractDbDialect implements IDbDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    @Override
    protected void createRequiredFunctions() {
        super.createRequiredFunctions();
        try {
            jdbcTemplate.queryForInt("select char_length(sym_escape('')) from rdb$database");
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                log.error("FirebirdSymUdfMissing");
            }
            throw new RuntimeException("FirebirdSymEscapeMissing", e);
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "',1) from rdb$database");
        if (nodeId != null) {
            jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE
                    + "','" + nodeId + "') from rdb$database");
        }
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "',null) from rdb$database");
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE
                + "',null) from rdb$database");
    }

    public String getSyncTriggersExpression() {
        return "rdb$get_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "') is null";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "current_transaction||''";
    }

    @Override
    protected String getTableNamePattern(String tableName) {
        /*
         * When looking up a table definition, Jaybird treats underscore (_) in
         * the table name as a wildcard, so it needs to be escaped, or you'll
         * get back column names for more than one table. Example:
         * DatabaseMetaData.metaData.getColumns(null, null, "SYM\\_NODE", null)
         */
        return tableName.replaceAll("\\_", "\\\\_");
    }

    @Override
    public boolean supportsReturningKeys() {
        return true;
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
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
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    public void purge() {
    }

    @Override
    public String getName() {
        return super.getName().substring(0, 49);
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }
    
    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }
    
}