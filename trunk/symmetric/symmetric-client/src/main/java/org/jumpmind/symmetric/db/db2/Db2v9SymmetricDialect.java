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

import java.net.URL;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.core.JdbcTemplate;

public class Db2v9SymmetricDialect extends Db2SymmetricDialect implements ISymmetricDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    public Db2v9SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        ISqlTransaction transaction = null;
        try {
            transaction = platform.getSqlTemplate().startSqlTransaction();
            enableSyncTriggers(transaction);
            transaction.commit();
        } catch (Exception e) {
            try {
                log.info("Creating environment variables %s and %s", SYNC_TRIGGERS_DISABLED_USER_VARIABLE,
                        SYNC_TRIGGERS_DISABLED_NODE_VARIABLE);
                new SqlScript(getSqlScriptUrl(), getPlatform().getSqlTemplate(), ";").execute();
            } catch (Exception ex) {
                log.error("Error while initializing DB2 dialect", ex);
            }
        } finally {
            close(transaction);
        }
    }
    
    private URL getSqlScriptUrl() {
        return getClass().getResource("/org/jumpmind/symmetric/db/db2.sql");
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    public String getSourceNodeExpression() {
        return SYNC_TRIGGERS_DISABLED_NODE_VARIABLE;
    }
    
}