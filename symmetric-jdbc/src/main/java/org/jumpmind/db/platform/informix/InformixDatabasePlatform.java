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
package org.jumpmind.db.platform.informix;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class InformixDatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    public static final String JDBC_DRIVER = "com.informix.jdbc.IfxDriver";

    public static final String JDBC_SUBPROTOCOL = "informix-sqli";

    private Map<String, String> sqlScriptReplacementTokens;

    public InformixDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);

        sqlScriptReplacementTokens = super.getSqlScriptReplacementTokens();
        if (sqlScriptReplacementTokens == null) {
                sqlScriptReplacementTokens = new HashMap<String, String>();
        }
        sqlScriptReplacementTokens.put("current_timestamp", "current");
    }

    @Override
    protected InformixDdlBuilder createDdlBuilder() {
        return new InformixDdlBuilder();
    }

    @Override
    protected InformixDdlReader createDdlReader() {
        return new InformixDdlReader(this);
    }

    @Override
    protected InformixJdbcSqlTemplate createSqlTemplate() {
        return new InformixJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.INFORMIX;
    }

    public String getDefaultCatalog() {
//        if (StringUtils.isBlank(defaultCatalog)) {
//            defaultCatalog = getSqlTemplate().queryForObject("select trim(sqc_currdb) from sysmaster:syssqlcurall where sqc_sessionid = dbinfo('sessionid')", String.class);
//        }
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = getSqlTemplate().queryForObject("select trim(user) from sysmaster:sysdual", String.class);
        }
        return defaultSchema;
    }

    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
            return sqlScriptReplacementTokens;
    }

    @Override
    public boolean isClob(int type) {
        return type == Types.CLOB;
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE TRIGGER TEST_TRIGGER DELETE ON " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " FOR EACH ROW(DELETE FROM " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + " WHERE TEST_ID IS NULL)";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, triggerSql);

        try {
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }

        return result;
    }
    
    @Override
    public boolean supportsLimitOffset() {
        return true;
    }
    
    @Override
    public String massageForLimitOffset(String sql, int limit, int offset) {
        if (StringUtils.containsIgnoreCase(sql, "select")) {
            sql = StringUtils.replaceIgnoreCase(sql, "select", "select skip " + offset + " first " + limit);
        }
        return sql;
    }
}
