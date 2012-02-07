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

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.UncategorizedSQLException;

/*
 * Database dialect for <a href="http://www.embarcadero.com/products/interbase/">Interbase</a>.
 */
public class InterbaseSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public static final String CONTEXT_TABLE_NAME = "context";

    static final String CONTEXT_TABLE_CREATE = "create global temporary table %s (id varchar(30), context_value varchar(30)) on commit preserve rows";

    static final String CONTEXT_TABLE_INSERT = "insert into %s (id, context_value) values (?, ?)";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    public InterbaseSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerText = new InterbaseTriggerTemplate();
    }
    
    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        try {
            platform.getSqlTemplate().queryForInt("select count(*) from " + contextTableName);
        } catch (Exception e) {
            try {
                log.info("Creating global temporary table {}", contextTableName);
                platform.getSqlTemplate().update(String.format(CONTEXT_TABLE_CREATE, contextTableName));
            } catch (Exception ex) {
                log.error("Error while initializing Interbase dialect", ex);
            }
        }
    }

    @Override
    protected void createRequiredFunctions() {
        super.createRequiredFunctions();
        try {
            platform.getSqlTemplate().queryForObject("select sym_escape('') from rdb$database", String.class);
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                log.error("Please install the sym_udf.so/dll to your {interbase_home}/UDF folder");
            }
            throw new RuntimeException("Function SYM_ESCAPE is not installed", e);
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
            SYNC_TRIGGERS_DISABLED_USER_VARIABLE, "1" });
        if (nodeId != null) {
            transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
                SYNC_TRIGGERS_DISABLED_NODE_VARIABLE, nodeId });
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        transaction.prepareAndExecute("delete from " + contextTableName);
    }

    public String getSyncTriggersExpression() {
        return ":" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    public String getSequenceName(SequenceIdentifier identifier) {
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
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void purge() {
    }

    @Override
    public String getName() {
        return super.getName();
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
        platform.getSqlTemplate().update("delete from " + tableName);
    }
        
    @Override
    public void cleanupTriggers() {
        List<String> names = platform.getSqlTemplate().query("select rdb$trigger_name from rdb$triggers where rdb$trigger_name like '"+parameterService.getTablePrefix().toUpperCase()+"_%'", new StringMapper());
        int count = 0;
        for (String name : names) {
            count += platform.getSqlTemplate().update("drop trigger " + name);
        }
        log.info("Remove {} triggers", count);
    }
}
