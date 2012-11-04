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
package org.jumpmind.symmetric.db.postgresql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Support for PostgreSQL
 */
public class PostgreSqlSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String TRANSACTION_ID_EXPRESSION = "txid_current()";

    static final String SYNC_TRIGGERS_DISABLED_VARIABLE = "symmetric.triggers_disabled";

    static final String SYNC_NODE_DISABLED_VARIABLE = "symmetric.node_disabled";
    
    static final String SQL_DROP_FUNCTION = "drop function $(defaultSchema)$(functionName)";
    
    static final String SQL_FUNCTION_INSTALLED = 
        " select count(*) from information_schema.routines " + 
        " where routine_name = '$(functionName)' and specific_schema = '$(defaultSchema)'" ;    

    private boolean supportsTransactionId = false;

    private String transactionIdExpression = "null";
        
    public PostgreSqlSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new PostgreSqlTriggerTemplate(this);
    }
    
    @Override
    protected void createRequiredFunctions() {
    	
        if (transactionIdSupported()) {
            supportsTransactionId = true;
            transactionIdExpression = TRANSACTION_ID_EXPRESSION;        	
        } else {
            log.warn("Capturing of transaction identifiers is not supported in this version of postgres");
        }

    	ISqlTransaction transaction = null;
        try {
            transaction = platform.getSqlTemplate().startSqlTransaction();
            enableSyncTriggers(transaction);
        } catch (Exception e) {
            String message = "Please add \"custom_variable_classes = 'symmetric'\" to your postgresql.conf file"; 
            log.error(message);
            throw new SymmetricException(message, e);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        
        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = "CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS INTEGER AS $$                                                                                                                     " + 
                    "                                DECLARE                                                                                                                                                                " + 
                    "                                  triggerDisabled INTEGER;                                                                                                                                             " + 
                    "                                BEGIN                                                                                                                                                                  " + 
                    "                                  select current_setting('symmetric.triggers_disabled') into triggerDisabled;                                                                                          " + 
                    "                                  return triggerDisabled;                                                                                                                                              " + 
                    "                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
                    "                                  return 0;                                                                                                                                                            " + 
                    "                                END;                                                                                                                                                                   " + 
                    "                                $$ LANGUAGE plpgsql;                                                                                                                                                   ";
            install(sql, triggersDisabled);
        }

        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "escape";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS VARCHAR AS $$                                                                                                                     " + 
                    "                                DECLARE                                                                                                                                                                " + 
                    "                                  nodeId VARCHAR(50);                                                                                                                                                  " + 
                    "                                BEGIN                                                                                                                                                                  " + 
                    "                                  select current_setting('symmetric.node_disabled') into nodeId;                                                                                                       " + 
                    "                                  return nodeId;                                                                                                                                                       " + 
                    "                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
                    "                                  return '';                                                                                                                                                           " + 
                    "                                END;                                                                                                                                                                   " + 
                    "                                $$ LANGUAGE plpgsql;                                                                                                                                                   ";
            install(sql, nodeDisabled);
        }

        String largeObjects = this.parameterService.getTablePrefix() + "_" + "escape";
        if (!installed(SQL_FUNCTION_INSTALLED, largeObjects)) {
            String sql = "CREATE OR REPLACE FUNCTION $(defaultSchema)$(functionName)(objectId oid) RETURNS text AS $$                                                                                                            " + 
                    "                                DECLARE                                                                                                                                                                " + 
                    "                                  encodedBlob text;                                                                                                                                                    " + 
                    "                                  encodedBlobPage text;                                                                                                                                                " + 
                    "                                BEGIN                                                                                                                                                                  " + 
                    "                                  encodedBlob := '';                                                                                                                                                   " + 
                    "                                  FOR encodedBlobPage IN SELECT pg_catalog.encode(data, 'escape')                                                                                                                 " + 
                    "                                  FROM pg_largeobject WHERE loid = objectId ORDER BY pageno LOOP                                                                                                       " + 
                    "                                    encodedBlob := encodedBlob || encodedBlobPage;                                                                                                                     " + 
                    "                                  END LOOP;                                                                                                                                                            " + 
                    "                                  RETURN pg_catalog.encode(pg_catalog.decode(encodedBlob, 'escape'), 'base64');                                                                                                              " + 
                    "                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
                    "                                  RETURN '';                                                                                                                                                           " + 
                    "                                END                                                                                                                                                                    " + 
                    "                                $$ LANGUAGE plpgsql;                                                                                                                                                   ";
            install(sql, largeObjects);
        }
        
    }
    
    @Override
    protected void dropRequiredFunctions() {
        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            uninstall(SQL_DROP_FUNCTION, triggersDisabled);
        }

        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "escape";
        if (installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            uninstall(SQL_DROP_FUNCTION, nodeDisabled);
        }

        String largeObjects = this.parameterService.getTablePrefix() + "_" + "escape";
        if (installed(SQL_FUNCTION_INSTALLED, largeObjects)) {
            uninstall(SQL_DROP_FUNCTION, largeObjects);
        }

    }

    private boolean transactionIdSupported() {
    	
    	boolean transactionIdSupported = false;
    	
    	if (platform.getSqlTemplate().queryForInt("select count(*) from information_schema.routines where routine_name='txid_current'") > 0) {
    		transactionIdSupported = true;
    	}
    	
    	return transactionIdSupported;
    }
    
    @Override
    public boolean requiresAutoCommitFalseToSetFetchSize() {
        return true;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) from information_schema.triggers where trigger_name = ? "
                + "and event_object_table = ? and trigger_schema = ?", new Object[] { triggerName.toLowerCase(),
                tableName, schema == null ? platform.getDefaultSchema() : schema }) > 0 ||
                platform.getSqlTemplate().queryForInt("select count(*) from information_schema.routines where routine_name = ? "
                        + "and routine_schema = ?", new Object[] { "f" + triggerName.toLowerCase(),
                        schema == null ? platform.getDefaultSchema() : schema }) > 0;
    }
    
    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String dropSql = "drop trigger " + triggerName + " on " + schemaName + tableName;
        logSql(dropSql, sqlBuffer);
        final String dropFunction = "drop function " + schemaName + "f" + triggerName + "()";
        logSql(dropFunction, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            String sql = null;
            try {
                sql = dropSql;
                platform.getSqlTemplate().update(dropSql);
            } catch (Exception e) {
                log.warn("Tried to remove trigger using: {} and failed because: {}", sql, e.getMessage());
            }
            try {                
                sql = dropFunction;
                platform.getSqlTemplate().update(dropFunction);
            } catch (Exception e) {
                log.warn("Tried to remove function using: {} and failed because: {}", sql, e.getMessage());
            }
        }
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("select set_config('" + SYNC_TRIGGERS_DISABLED_VARIABLE + "', '1', false)");
        if (nodeId == null) {
            nodeId = "";
        }
        transaction.prepareAndExecute("select set_config('" + SYNC_NODE_DISABLED_VARIABLE + "', '" + nodeId + "', false)");
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("select set_config('" + SYNC_TRIGGERS_DISABLED_VARIABLE
                + "', '', false)");
        transaction.prepareAndExecute("select set_config('" + SYNC_NODE_DISABLED_VARIABLE
                + "', '', false)");
    }

    public String getSyncTriggersExpression() {
        return "$(defaultSchema)" + parameterService.getTablePrefix() + "_triggers_disabled() = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return transactionIdExpression;
    }

    @Override
    public boolean supportsTransactionId() {
        return supportsTransactionId;
    }

    public void purge() {
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }    
    
}
