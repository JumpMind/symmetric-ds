/**
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

package org.jumpmind.symmetric.db.ase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Sybase dialect was tested with jconn4 JDBC driver.
 * Based on the MsSqlSymmetricDialect.
 *
 *  disk resize name = master, size = 16384
 *  create database symmetricclient on master = '30M'
 */
public class AseSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String SQL_DROP_FUNCTION = "drop function dbo.$(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(object_name(object_id('$(functionName)')))" ;

    public AseSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        if (getMajorVersion() >= 16) {
            this.triggerTemplate = new Ase16TriggerTemplate(this);
        } else {
            this.triggerTemplate = new AseTriggerTemplate(this);
        }
    }

    @Override
    public boolean createOrAlterTablesIfNecessary(String... tableNames) {
        boolean altered = super.createOrAlterTablesIfNecessary(tableNames);
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();
        String prefix = getTablePrefix();

        try {
            int changeIdentityGap = parameterService.getInt(ParameterConstants.SYBASE_CHANGE_IDENTITY_GAP, 1000);
            if (changeIdentityGap > 0) {
                String dataTable = TableConstants.getTableName(prefix, TableConstants.SYM_DATA).toLowerCase();
                String sql = "select max(i.identitygap) from sysindexes i inner join sysobjects t on t.id = i.id where t.name = ?";
                long identityGap = sqlTemplate.queryForLong(sql, dataTable);
                if (identityGap != changeIdentityGap) {
                    log.info("Changing identity gap for {} to {}", dataTable, changeIdentityGap);
                    sqlTemplate.update("sp_chgattribute " + dataTable + ", 'identity_gap', " + changeIdentityGap);
                    altered = true;
                }
            }
        } catch (Exception e) {            
            log.warn("Failed to alter identity gap: {}", e.getMessage());
            log.debug("", e);
        }

        try {
            if (parameterService.is(ParameterConstants.SYBASE_ROW_LEVEL_LOCKS_ONLY, true)) {
                List<String> tables = new ArrayList<String>();
                tables.add(TableConstants.getTableName(prefix, TableConstants.SYM_DATA).toLowerCase());
                tables.add(TableConstants.getTableName(prefix, TableConstants.SYM_DATA_EVENT).toLowerCase());
                tables.add(TableConstants.getTableName(prefix, TableConstants.SYM_OUTGOING_BATCH).toLowerCase());
                tables.add(TableConstants.getTableName(prefix, TableConstants.SYM_MONITOR_EVENT).toLowerCase());
                String sql = "select case (sysstat2 & 57344) when 32768 then 1 else 0 end from sysobjects where name = ?";

                for (String table : tables) {
                    if (sqlTemplate.queryForInt(sql, table) == 0) {
                        log.info("Altering {} for row-level locking", table);
                        sqlTemplate.update("alter table " + table + " lock datarows");
                        altered = true;
                    }
                }
            }
        } catch (Exception e) {            
            log.warn("Failed to alter row-level locking: {}", e.getMessage());
            log.debug("", e);
        }
        return altered;
    }

    @Override
    public void createRequiredDatabaseObjects() {
    }

    @Override
    public void dropRequiredDatabaseObjects() {
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, final String catalogName, String schemaName,
            final String triggerName, String tableName, ISqlTransaction transaction) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String sql = "drop trigger " + schemaName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {

          ((JdbcSqlTransaction) transaction)
            .executeCallback(new IConnectionCallback<Boolean>() {
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
                        throw e;
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
    protected boolean doesTriggerExistOnPlatform(final String catalogName, final String schema, final String tableName,
            final String triggerName) {
        return ((JdbcSqlTemplate) platform.getSqlTemplate())
        .execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                String previousCatalog = con.getCatalog();
                PreparedStatement stmt = con
                        .prepareStatement("select count(*) from dbo.sysobjects where type = 'TR' AND name = ?");
                try {
                    if (catalogName != null) {
                        con.setCatalog(catalogName);
                    }
                    stmt.setString(1, triggerName);
                    ResultSet rs = null;
                    try {                        
                        rs = stmt.executeQuery();
                    } catch (Exception ex) {
                        try {                            
                            stmt.close();
                        } catch (Exception exClose) {
                            log.debug("Falied to close stmt", exClose);
                        }
                        if (catalogName != null) {                            
                            log.debug("Tried: select count(*) from dbo.sysobjects where type = 'TR' AND name ='{}' which failed, will try again with catalog.", triggerName);
                            // try again with the source catalog prefixed.
                            try {
                                PreparedStatement stmt2 = con
                                        .prepareStatement(String.format("select count(*) from %s.dbo.sysobjects where type = 'TR' AND name = ?", catalogName));
                                stmt2.setString(1, triggerName);
                                log.debug(String.format("TRY AGAIN Exceute:  select count(*) from %s.dbo.sysobjects where type = 'TR' AND name ='%s'", catalogName, triggerName));
                                rs = stmt2.executeQuery();
                            } catch (Exception ex2) {
                                log.error(String.format("Failed again with catalog... select count(*) from %s.dbo.sysobjects where type = 'TR' AND name = '%s'", catalogName, triggerName), ex2);
                                throw new RuntimeException(String.format("Detect trigger query failed. select count(*) from dbo.sysobjects where type = 'TR' AND name ='%s'", triggerName), ex);
                            }
                        } else {
                            throw new RuntimeException(String.format("Detect trigger query failed. select count(*) from dbo.sysobjects where type = 'TR' AND name ='%s'", triggerName), ex);
                        }
                        
                    }
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
        return "@clientapplname <> 'SymmetricDS'";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "bintostr(xactkey) from master.dbo.systransactions where spid = @@spid";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return true;
    }

    public void cleanDatabase() {
        platform.getSqlTemplate().update("dump transaction "+platform.getDefaultCatalog()+" with no_log");
    }

    public boolean needsToSelectLobData() {
        return true;
    }

    @Override
    public int getMaxTriggerNameLength() {
        return 28;
    }

}
