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


package org.jumpmind.symmetric.db.sybase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Sybase dialect was tested with jconn4 JDBC driver.
 * Based on the MsSqlSymmetricDialect.
 *
 *  disk resize name = master, size = 16384
 *  create database symmetricclient on master = '30M'
 */
public class SybaseSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String SQL_DROP_FUNCTION = "drop function dbo.$(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(object_name(object_id('$(functionName)')))" ;

    public SybaseSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new SybaseTriggerTemplate(this);
    }

    @Override
    protected void createRequiredDatabaseObjects() {

        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = "create function dbo.$(functionName)(@unused smallint) returns smallint as                                                                                                                              " +
                    "                                begin                                                                                                                                                                  " +
                    "                                    declare @clientapplname varchar(50)                                                                                                                                " +
                    "                                    select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid                                                                           " +
                    "                                    if @clientapplname = 'SymmetricDS'                                                                                                                                 " +
                    "                                        return 1                                                                                                                                                       " +
                    "                                    return 0                                                                                                                                                           " +
                    "                                end                                                                                                                                                                    ";
            install(sql, triggersDisabled);
        }

        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "create function dbo.$(functionName)(@unused smallint) returns varchar(50) as                                                                                                                           " +
                    "                                begin                                                                                                                                                                  " +
                    "                                    declare @clientname varchar(50)                                                                                                                                    " +
                    "                                    select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'                                                " +
                    "                                    return @clientname                                                                                                                                                 " +
                    "                                end                                                                                                                                                                    ";
            install(sql, nodeDisabled);
        }

        String txId = this.parameterService.getTablePrefix() + "_" + "txid";
        if (!installed(SQL_FUNCTION_INSTALLED, txId)) {
            String sql = "create function dbo.$(functionName)(@unused smallint) returns varchar(50) as                                                                                                                           " +
                    "                                begin                                                                                                                                                                  " +
                    "                                    declare @txid varchar(50)                                                                                                                                          " +
                    "                                    if (@@TRANCOUNT > 0) begin                                                                                                                                         " +
                    "                                        select @txid = convert(varchar, starttime, 20) + '.' + convert(varchar, loid) from master.dbo.systransactions where spid = @@spid                              " +
                    "                                    end                                                                                                                                                                " +
                    "                                    return @txid                                                                                                                                                       " +
                    "                                end                                                                                                                                                                    ";
            install(sql, txId);
        }

    }

    @Override
    protected void dropRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base64_encode";
        if (installed(SQL_FUNCTION_INSTALLED, encode)) {
            uninstall(SQL_DROP_FUNCTION, encode);
        }

        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            uninstall(SQL_DROP_FUNCTION, triggersDisabled);
        }
        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            uninstall(SQL_DROP_FUNCTION, nodeDisabled);
        }
        String txId = this.parameterService.getTablePrefix() + "_" + "txid";
        if (installed(SQL_FUNCTION_INSTALLED, txId)) {
            uninstall(SQL_DROP_FUNCTION, txId);
        }

    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, final String catalogName, String schemaName,
            final String triggerName, String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String sql = "drop trigger " + schemaName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            ((JdbcSqlTemplate) platform.getSqlTemplate())
            .execute(new IConnectionCallback<Boolean>() {
                public Boolean execute(Connection con) throws SQLException {
                    String previousCatalog = con.getCatalog();
                    Statement stmt = null;
                    try {
                        if (catalogName != null) {
                            con.setCatalog(catalogName);
                        }
                        stmt = con.createStatement();
                        stmt.execute(sql);
                    } catch (Exception e) {
                        log.warn("Error removing {}: {}", triggerName, e.getMessage());
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
    protected boolean doesTriggerExistOnPlatform(final String catalogName, String schema, String tableName,
            final String triggerName) {
        return ((JdbcSqlTemplate) platform.getSqlTemplate())
        .execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
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

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        if (nodeId == null) {
            nodeId = "";
        }
        transaction.prepareAndExecute("set clientapplname 'SymmetricDS'");
        transaction.prepareAndExecute("set clientname '" + nodeId + "'");
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("set clientapplname null");
        transaction.prepareAndExecute("set clientname null");
    }

    public String getSyncTriggersExpression() {
        return "$(defaultCatalog)dbo."+parameterService.getTablePrefix()+"_triggers_disabled(0) = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
    	return platform.getDefaultCatalog() + ".dbo."+parameterService.getTablePrefix()+"_txid(0)";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

    public void purgeRecycleBin() {
    }

    public boolean needsToSelectLobData() {
        return true;
    }

}
