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

package org.jumpmind.symmetric.db.informix;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.AutoIncrementColumnFilter;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;

public class InformixDbDialect extends AbstractDbDialect implements IDbDialect {

    private String identifierQuoteString = "";

    private Map<String, String> sqlScriptReplacementTokens;

    public InformixDbDialect() {
        Map<String, String> env = System.getenv();
        String clientIdentifierMode = env.get("DELIMIDENT");
        if (clientIdentifierMode != null && clientIdentifierMode.equalsIgnoreCase("y")) {
            identifierQuoteString = "\"";
        }
        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "current");
    }

    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    @Override
    public IColumnFilter newDatabaseColumnFilter() {
        return new AutoIncrementColumnFilter();
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return jdbcTemplate.queryForInt(
                "select count(*) from systriggers where lower(trigname) = ?",
                new Object[] { triggerName.toLowerCase() }) > 0;
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        jdbcTemplate.queryForList("select " + tablePrefix + "_triggers_set_disabled('t'), "
                + tablePrefix + "_node_set_disabled(?) from sysmaster:sysdual",
                new Object[] { nodeId });
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.queryForList("select " + tablePrefix + "_triggers_set_disabled('f'), "
                + tablePrefix + "_node_set_disabled(null) from sysmaster:sysdual");
    }

    public String getSyncTriggersExpression() {
        return "not $(defaultSchema)" + tablePrefix + "_triggers_disabled()";
    }

    @Override
    public boolean supportsTransactionId() {
        // TODO: write a user-defined routine in C that calls
        // mi_get_transaction_id()
        return false;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    @Override
    public boolean isBlobSyncSupported() {
        return false;
    }

    @Override
    public boolean isClobSyncSupported() {
        return false;
    }

    @Override
    public boolean allowsNullForIdentityColumn() {
        return false;
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

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        String defaultSchema = super.getDefaultSchema();
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = jdbcTemplate.queryForObject("select trim(user) from sysmaster:sysdual",
                    String.class);
        }
        return defaultSchema;
    }

    @Override
    public String getIdentifierQuoteString() {
        return identifierQuoteString;
    }

    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }
}