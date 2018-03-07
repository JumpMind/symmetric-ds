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
package org.jumpmind.symmetric.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.SymmetricUtils;

public class MySqlSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    private static final String PRE_5_1_23 = "_pre_5_1_23";
    
    private static final String PRE_5_7_6 = "_pre_5_7_6";

    private static final String POST_5_7_6 = "_post_5_7_6";

    private static final String TRANSACTION_ID = "transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";

    static final String SQL_DROP_FUNCTION = "drop function $(functionName)";
    
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from information_schema.routines where routine_name='$(functionName)' and routine_schema in (select database())" ;

    private String functionTemplateKeySuffix = null;

    public MySqlSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MySqlTriggerTemplate(this);
        this.parameterService = parameterService;
        
        int[] versions = Version.parseVersion(getProductVersion());        
        if (getMajorVersion() == 5
                && (getMinorVersion() == 0 || (getMinorVersion() == 1 && versions[2] < 23))) {
            this.functionTemplateKeySuffix = PRE_5_1_23;    
        } else if (getMajorVersion() == 5
                && (getMinorVersion() < 7 || (getMinorVersion() == 7 && versions[2] < 6))) {
        	this.functionTemplateKeySuffix = PRE_5_7_6;
        } else {
            this.functionTemplateKeySuffix = POST_5_7_6;
        }        
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }        

    @Override
    public void createRequiredDatabaseObjects() {
        if (this.functionTemplateKeySuffix.equals(PRE_5_1_23)) {
            String function = this.parameterService.getTablePrefix() + "_" + TRANSACTION_ID + this.functionTemplateKeySuffix;
            if (!installed(SQL_FUNCTION_INSTALLED, function)) {
            	String sql = "create function $(functionName)()                                                        " + 
                        " returns varchar(50) NOT DETERMINISTIC READS SQL DATA                                                        " + 
                        " begin                                                        " +
                        " declare comm_name varchar(50);                                                        " + 
                        " declare comm_value varchar(50);                                                        " + 
                        " declare comm_cur cursor for show status like 'Com_commit';                                                        " + 
                        " if @@autocommit = 0 then                                                        " + 
                        " open comm_cur;                                                        " + 
                        " fetch comm_cur into comm_name, comm_value;                                                        " + 
                        " close comm_cur;                                                        " + 
                        " return concat(concat(connection_id(), '.'), comm_value);                                                        " + 
                        " else                                                        " + 
                        " return null;                                                        " + 
                        " end if;                                                          " + 
                        " end                                                             ";
                install(sql, function);
            }        
        } else if (this.functionTemplateKeySuffix.equals(PRE_5_7_6)){
            String function = this.parameterService.getTablePrefix() + "_" + TRANSACTION_ID + this.functionTemplateKeySuffix;
            if (!installed(SQL_FUNCTION_INSTALLED, function)) {
            	 String sql = "create function $(functionName)()                                                                                          \n" + 
                         " returns varchar(50) NOT DETERMINISTIC READS SQL DATA                                                                           \n" + 
                         " begin                                                                                                                          \n" + 
                         "    declare comm_value varchar(50);                                                                                             \n" + 
                         "    declare comm_cur cursor for select VARIABLE_VALUE from INFORMATION_SCHEMA.SESSION_STATUS where VARIABLE_NAME='COM_COMMIT';  \n" + 
                         "    open comm_cur;                                                                                                              \n" + 
                         "    fetch comm_cur into comm_value;                                                                                             \n" + 
                         "    close comm_cur;                                                                                                             \n" + 
                         "    return concat(concat(connection_id(), '.'), comm_value);                                                                    \n" + 
                         " end                                                                                                                            \n";
                install(sql, function);
            }                    
        } else {
            String function = this.parameterService.getTablePrefix() + "_" + TRANSACTION_ID + this.functionTemplateKeySuffix;
            if (!installed(SQL_FUNCTION_INSTALLED, function)) {
            	String sql = "create function $(functionName)()                                                                                           \n" + 
            			" returns varchar(50) NOT DETERMINISTIC READS SQL DATA                                                                            \n" + 
            			" begin                                                                                                                           \n" + 
            			"    declare done int default 0;                                                                                                  \n" +
            			"    declare comm_value varchar(50);                                                                                              \n" + 
            			"    declare comm_cur cursor for select TRX_ID from INFORMATION_SCHEMA.INNODB_TRX where TRX_MYSQL_THREAD_ID = CONNECTION_ID();    \n" + 
            			"    declare continue handler for not found set done = 1;                                                                         \n" +
            			"    open comm_cur;                                                                                                               \n" + 
            			"    fetch comm_cur into comm_value;                                                                                              \n" + 
            			"    close comm_cur;                                                                                                              \n" + 
            			"    return concat(concat(connection_id(), '.'), comm_value);                                                                     \n" + 
            			" end                                                                                                                             \n";
                install(sql, function);
            }                    
        }
    }
    
    @Override
    public void dropRequiredDatabaseObjects() {
        String function = this.parameterService.getTablePrefix() + "_" + TRANSACTION_ID + this.functionTemplateKeySuffix;
        if (installed(SQL_FUNCTION_INSTALLED, function)) {
            uninstall(SQL_DROP_FUNCTION, function);
        }        
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        catalog = catalog == null ? (platform.getDefaultCatalog() == null ? null : platform
                .getDefaultCatalog()) : catalog;
        String checkCatalogSql = (catalog != null && catalog.length() > 0) ? " and trigger_schema='"
                + catalog + "'"
                : "";
        return platform
                .getSqlTemplate()
                .queryForInt(
                        "select count(*) from information_schema.triggers where trigger_name like ? and event_object_table like ?"
                                + checkCatalogSql, new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName, ISqlTransaction transaction) {
        catalogName = catalogName == null ? "" : (catalogName + ".");
        final String sql = "drop trigger if exists " + catalogName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            transaction.execute(sql);
        }
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            transaction
                    .prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        transaction.prepareAndExecute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }

    private final String getTransactionFunctionName() {        
        return SymmetricUtils.quote(this, platform.getDefaultCatalog()) + "." + parameterService.getTablePrefix() + "_"
                + TRANSACTION_ID + this.functionTemplateKeySuffix;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return getTransactionFunctionName() + "()";
    }

    public void cleanDatabase() {
    }

    @Override
    protected String switchCatalogForTriggerInstall(String catalog, ISqlTransaction transaction) {
        if (catalog != null) {
            Connection c = ((JdbcSqlTransaction) transaction).getConnection();
            String previousCatalog;
            try {
                previousCatalog = c.getCatalog();
                c.setCatalog(catalog);
                return previousCatalog;
            } catch (SQLException e) {
                throw new SqlException(e);
            }
        } else {
            return null;
        }
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_old_data is null or var_row_data != var_old_data";
    }
    
    @Override
    public PermissionType[] getSymTablePermissions() {
        PermissionType[] permissions = { PermissionType.CREATE_TABLE, PermissionType.DROP_TABLE, PermissionType.CREATE_TRIGGER, PermissionType.DROP_TRIGGER, PermissionType.CREATE_ROUTINE};
        return permissions;
    }
}
