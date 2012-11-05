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

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.UncategorizedSQLException;

/*
 * Database dialect for <a href="http://www.firebirdsql.org/">Firebird</a>.
 */
public class FirebirdSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String SQL_DROP_FUNCTION = "DROP EXTERNAL FUNCTION $(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from rdb$functions where rdb$function_name = upper('$(functionName)')" ;

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    public FirebirdSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new FirebirdTriggerTemplate(this);
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {
        String escape = this.parameterService.getTablePrefix() + "_" + "escape";
        if (!installed(SQL_FUNCTION_INSTALLED, escape)) {
            String sql = "declare external function $(functionName) cstring(32660)                                                                                                                                               " + 
                    "  returns cstring(32660) free_it entry_point 'sym_escape' module_name 'sym_udf'                                                                                          ";
            install(sql, escape);
        }
        
        String hex = this.parameterService.getTablePrefix() + "_" + "hex";
        if (!installed(SQL_FUNCTION_INSTALLED, hex)) {
            String sql = "declare external function $(functionName) blob                                                                                                                                                         " + 
                    "  returns cstring(32660) free_it entry_point 'sym_hex' module_name 'sym_udf'                                                                                             ";
            install(sql, hex);
        }        
        
        try {
            platform.getSqlTemplate().queryForInt("select char_length("+escape+"('')) from rdb$database");
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                log.error("Please install the sym_udf.so/dll to your {firebird_home}/UDF folder");
            }
            throw new RuntimeException("Function "+escape+" is not installed", e);
        }
    }
    
    @Override
    protected void dropRequiredDatabaseObjects() {
        String escape = this.parameterService.getTablePrefix() + "_" + "escape";
        if (installed(SQL_FUNCTION_INSTALLED, escape)) {
            uninstall(SQL_DROP_FUNCTION, escape);
        }
        
        String hex = this.parameterService.getTablePrefix() + "_" + "hex";
        if (installed(SQL_FUNCTION_INSTALLED, hex)) {
            uninstall(SQL_DROP_FUNCTION, hex);
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "',1) from rdb$database");
        if (nodeId != null) {
            transaction.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE
                    + "','" + nodeId + "') from rdb$database");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "',null) from rdb$database");
        transaction.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE
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
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void purgeRecycleBin() {
    }

    @Override
    public String getName() {
        return super.getName().substring(0, 49);
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
        platform.getSqlTemplate().update("delete from " + tableName);
    }

    @Override
    public void cleanupTriggers() {        
        List<String> names = platform.getSqlTemplate().query("select rdb$trigger_name from rdb$triggers where rdb$trigger_name like '"+parameterService.getTablePrefix().toUpperCase()+"_%'", new StringMapper());
        int count = 0;
        for (String name : names) {
            count += platform.getSqlTemplate().update("drop trigger " + name);
        }
        if (count > 0) {
            log.info("Remove {} triggers", count);
        }
    }
}
