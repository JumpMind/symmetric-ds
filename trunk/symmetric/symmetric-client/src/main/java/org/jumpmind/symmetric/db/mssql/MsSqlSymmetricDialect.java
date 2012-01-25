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

package org.jumpmind.symmetric.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.jdbc.IConnectionCallback;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.jumpmind.db.sql.jdbc.JdbcSqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * This dialect was tested with the jTDS JDBC driver on SQL Server 2005.
 * 
 * TODO support text and image fields, they cannot be referenced from the
 * inserted or deleted tables in the triggers. Here is one idea we could
 * implement: http://www.devx.com/getHelpOn/10MinuteSolution/16544
 */
public class MsSqlSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public MsSqlSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerText = new MsSqlTriggerTemplate();
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
        return BinaryEncoding.BASE64;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(final String catalogName, String schema,
            String tableName, final String triggerName) {
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
        transaction.execute("DECLARE @CI VarBinary(128);" + "SET @CI=cast ('1" + nodeId
                + "' as varbinary(128));" + "SET context_info @CI;");
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.execute("set context_info 0x0");
    }

    public String getSyncTriggersExpression() {
        return "$(defaultCatalog)dbo." + parameterService.getTablePrefix()
                + "_triggers_disabled() = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "@TransactionId";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

    /*
     * Nothing to do for SQL Server
     */
    public void purge() {
    }

    public boolean needsToSelectLobData() {
        return true;
    }

}
