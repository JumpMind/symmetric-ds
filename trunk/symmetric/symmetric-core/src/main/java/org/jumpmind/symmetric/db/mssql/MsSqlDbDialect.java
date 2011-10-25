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


package org.jumpmind.symmetric.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.AutoIncrementColumnFilter;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/*
 * This dialect was tested with the jTDS JDBC driver on SQL Server 2005.
 * 
 * TODO support text and image fields, they cannot be referenced from the
 * inserted or deleted tables in the triggers. Here is one idea we could
 * implement: http://www.devx.com/getHelpOn/10MinuteSolution/16544
 */
public class MsSqlDbDialect extends AbstractDbDialect implements IDbDialect {

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }    

    @Override
    public IColumnFilter newDatabaseColumnFilter() {
        return new AutoIncrementColumnFilter();
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, final String catalogName, String schemaName,
            final String triggerName, String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String sql = "drop trigger " + schemaName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            jdbcTemplate.execute(new ConnectionCallback<Object>() {
                public Object doInConnection(Connection con) throws SQLException, DataAccessException {
                    String previousCatalog = con.getCatalog();
                    Statement stmt = null;
                    try {
                        if (catalogName != null) {
                            con.setCatalog(catalogName);
                        }
                        stmt = con.createStatement();
                        stmt.execute(sql);
                    } catch (Exception e) {
                        log.warn("TriggerDropError", triggerName, e.getMessage());
                    } finally {
                        if (catalogName != null) {
                            con.setCatalog(previousCatalog);
                        }
                        try {
                            stmt.close();
                        } catch (Exception e) {
                        }
                    }
                    return Boolean.FALSE;
                }
            });
        }
    }

    @Override
    protected String switchCatalogForTriggerInstall(String catalog, Connection c) throws SQLException {
        if (catalog != null) {
            String previousCatalog = c.getCatalog();
            c.setCatalog(catalog);
            return previousCatalog;
        } else {
            return null;
        }
    }

    @Override
    public void allowIdentityInserts(JdbcTemplate template, Table table) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            template.execute("SET IDENTITY_INSERT " + table.getFullyQualifiedTableName() + " ON");
        }
    }

    @Override
    public void revertAllowIdentityInserts(JdbcTemplate template, Table table) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            template.execute("SET IDENTITY_INSERT " + table.getFullyQualifiedTableName() + " OFF");
        }
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(final String catalogName, String schema, String tableName,
            final String triggerName) {
        return jdbcTemplate.execute(new ConnectionCallback<Boolean>() {
            public Boolean doInConnection(Connection con) throws SQLException, DataAccessException {
                String previousCatalog = con.getCatalog();
                PreparedStatement stmt = con
                        .prepareStatement("select count(*) from sysobjects where type = 'TR' AND name = ?");
                try {
                    if (catalogName != null) {
                        con.setCatalog(catalogName);
                    }
                    stmt.setString(1, triggerName);
                    ResultSet rs = stmt.executeQuery();
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        return count > 0;
                    }
                } finally {
                    if (catalogName != null) {
                        con.setCatalog(previousCatalog);
                    }
                    stmt.close();
                }
                return Boolean.FALSE;
            }
        });
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        if (nodeId == null) {
            nodeId = "";
        }
        jdbcTemplate.update("DECLARE @CI VarBinary(128);" + "SET @CI=cast ('1" + nodeId + "' as varbinary(128));"
                + "SET context_info @CI;");
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update("set context_info 0x0");
    }

    public String getSyncTriggersExpression() {
        return "$(defaultCatalog)dbo."+tablePrefix+"_triggers_disabled() = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "@TransactionId";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    /*
     * SQL Server always pads character fields out to the right to fill out
     * field with space characters.
     * 
     * @return true always
     */
    public boolean isNonBlankCharColumnSpacePadded() {
        return true;
    }

    /*
     * @return false always
     */
    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

    @Override
    public boolean isDateOverrideToTimestamp() {
        return true;
    }

    /*
     * SQL Server pads an empty string with spaces.
     * 
     * @return false always
     */
    public boolean isEmptyStringNulled() {
        return false;
    }

    /*
     * Nothing to do for SQL Server
     */
    public void purge() {
    }

    public String getDefaultCatalog() {
        return (String) jdbcTemplate.queryForObject("select DB_NAME()", String.class);
    }

    @Override
    public String getDefaultSchema() {
        String defaultSchema = super.getDefaultSchema();
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) jdbcTemplate.queryForObject("select SCHEMA_NAME()", String.class);
        }
        return defaultSchema;
    }

    public boolean needsToSelectLobData() {
        return true;
    }

}