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
package org.jumpmind.symmetric.db.interbase;

import java.util.List;

import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SingleColumnRowMapper;

/*
 * Database dialect for <a href="http://www.embarcadero.com/products/interbase/">Interbase</a>.
 */

public class InterbaseDbDialect extends AbstractDbDialect implements IDbDialect {

    public static final String CONTEXT_TABLE_NAME = "context";

    static final String CONTEXT_TABLE_CREATE = "create global temporary table %s (id varchar(30), context_value varchar(30)) on commit preserve rows";

    static final String CONTEXT_TABLE_INSERT = "insert into %s (id, context_value) values (?, ?)";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        String contextTableName = tablePrefix + "_" + CONTEXT_TABLE_NAME;
        try {
            jdbcTemplate.queryForInt("select count(*) from " + contextTableName);
        } catch (Exception e) {
            try {
                log.info("GlobalTempTableCreating", contextTableName);
                jdbcTemplate.execute(String.format(CONTEXT_TABLE_CREATE, contextTableName));
            } catch (Exception ex) {
                log.error("InterbaseDialectInitializingError", ex);
            }
        }
    }

    @Override
    protected void createRequiredFunctions() {
        super.createRequiredFunctions();
        try {
            jdbcTemplate.queryForObject("select sym_escape('') from rdb$database", String.class);
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                log.error("InterbaseSymUdfMissing");
            }
            throw new RuntimeException("InterbaseSymEscapeMissing", e);
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        String contextTableName = tablePrefix + "_" + CONTEXT_TABLE_NAME;
        jdbcTemplate.update(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
            SYNC_TRIGGERS_DISABLED_USER_VARIABLE, "1" });
        if (nodeId != null) {
            jdbcTemplate.update(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
                SYNC_TRIGGERS_DISABLED_NODE_VARIABLE, nodeId });
        }
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        String contextTableName = tablePrefix + "_" + CONTEXT_TABLE_NAME;
        jdbcTemplate.update("delete from " + contextTableName);
    }

    public String getSyncTriggersExpression() {
        return ":" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    protected String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case OUTGOING_BATCH:
            return "SYM_OUTGOING_BATCH_BATCH_ID";
        case DATA:
            return "SYM_DATA_DATA_ID";
        case TRIGGER_HIST:
            return "SYM_TRIGGER_TRIGGER_HIST_ID";
        }
        return null;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "null";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select gen_id(gen_" + sequenceName + ", 0) from rdb$database";
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
    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    public void purge() {
    }

    @Override
    public String getName() {
        return super.getName();
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
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        // Interbase and interclient driver do have support for batch updates, 
        // but we get primary/unique key violation when enabling its use
        return false;
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }
    
    
    @Override
    public void cleanupTriggers() {
        List<String> names = jdbcTemplate.query("select rdb$trigger_name from rdb$triggers where rdb$trigger_name like '"+tablePrefix.toUpperCase()+"_%'", new SingleColumnRowMapper<String>());
        int count = 0;
        for (String name : names) {
            count += jdbcTemplate.update("drop trigger " + name);
        }
        log.info("RemovedTriggers", count);
    }
}
