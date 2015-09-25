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
package org.jumpmind.symmetric.db.sqlite;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;

import com.mysql.jdbc.StringUtils;

public class SqliteSymmetricDialect extends AbstractSymmetricDialect {

    public static final String CONTEXT_TABLE_NAME = "context";
    
    static final String CONTEXT_TABLE_CREATE = "create table if not exists %s (id varchar(30), context_value varchar(30))";
    static final String CONTEXT_TABLE_CLEAR = "delete from %s;";

    static final String CONTEXT_TABLE_INSERT = "insert into %s (id, context_value) values (?, ?)";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";
    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";
    
    String sqliteFunctionToOverride;
    
    public SqliteSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new SqliteTriggerTemplate(this);
        
        sqliteFunctionToOverride = parameterService.getString(ParameterConstants.SQLITE_TRIGGER_FUNCTION_TO_USE);
    }
    
    @Override
    public void createRequiredDatabaseObjects() {
    	if(!StringUtils.isNullOrEmpty(sqliteFunctionToOverride)){
    		return;
    	}
    	
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        try {
            platform.getSqlTemplate().queryForInt("select count(*) from " + contextTableName);
        } catch (Exception e) {
            try {
                log.info("Creating context table {}", contextTableName);
                platform.getSqlTemplate().update(String.format(CONTEXT_TABLE_CREATE, contextTableName));
                platform.getSqlTemplate().update(String.format(CONTEXT_TABLE_CLEAR, contextTableName));
            } catch (Exception ex) {
                log.error("Error while initializing SQLite dialect", ex);
            }
        }
    }
    
    @Override
    public void dropRequiredDatabaseObjects() {
    }

    public void cleanDatabase() {
    }
    
    protected void setSqliteFunctionResult(ISqlTransaction transaction, final String name, final String result) {
        
    }
    
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    	if(StringUtils.isNullOrEmpty(sqliteFunctionToOverride)){
    		String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
            transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
                SYNC_TRIGGERS_DISABLED_USER_VARIABLE, "1" });
            if (nodeId != null) {
                transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
                    SYNC_TRIGGERS_DISABLED_NODE_VARIABLE, nodeId });
            }
    	}else{
	    	String node = "";
	    	if(nodeId != null){
	    		node = ":" + nodeId;
	    	}
	    	
	    	setSqliteFunctionResult(transaction, sqliteFunctionToOverride, "DISABLED" + node);
    	}

    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
    	if(StringUtils.isNullOrEmpty(sqliteFunctionToOverride)){
    		String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
            transaction.prepareAndExecute("delete from " + contextTableName);
    	}else{
    		setSqliteFunctionResult(transaction, sqliteFunctionToOverride, "ENABLED");
    	}
    }

    public String getSyncTriggersExpression() {
        if(StringUtils.isNullOrEmpty(sqliteFunctionToOverride)){
        	String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        	return "(not exists (select context_value from "+contextTableName+" where id = 'sync_triggers_disabled'))";
    	}else{
    		return "("+sqliteFunctionToOverride+"() not like 'DISABLED%')";
    	}
    }
    

    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt(
                "select count(*) from sqlite_master where type='trigger' and name=? and tbl_name=?", triggerName,
                tableName) > 0;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
     
    public boolean isBlobSyncSupported() {
        return true;
    }

    public boolean isClobSyncSupported() {
        return true;
    }
    
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }
    
    @Override
    public void truncateTable(String tableName) {
        String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
                .getDatabaseInfo().getDelimiterToken() : "";
         boolean success = false;
         int tryCount = 5;
         while (!success && tryCount > 0) {
             try {
                 Table table = platform.getTableFromCache(tableName, false);
                 if (table != null) {
                     platform.getSqlTemplate().update(
                             String.format("delete from %s%s%s", quote, table.getName(), quote));
                     success = true;
                 } else {
                     throw new RuntimeException(String.format("Could not find %s to trunate",
                             tableName));
                 }
             } catch (SqlException ex) {
                 log.warn("Failed to truncate the " + tableName + " table", ex);
                 AppUtils.sleep(5000);
                 tryCount--;
             }
         }
    }
    
    @Override
    public boolean canGapsOccurInCapturedDataIds() {
        return false;
    }
    
}
