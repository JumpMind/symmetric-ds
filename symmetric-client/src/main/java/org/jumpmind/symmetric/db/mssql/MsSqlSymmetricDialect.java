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
package org.jumpmind.symmetric.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
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

    static final protected String SQL_DROP_FUNCTION = "drop function dbo.$(functionName)";
    static final protected String SQL_FUNCTION_INSTALLED = "select count(object_name(object_id('$(functionName)')))" ;

    protected Boolean supportsDisableTriggers = null;

    public MsSqlSymmetricDialect() {
        super();
    }

    public MsSqlSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSqlTriggerTemplate(this);
    }
    
    @Override
    public Database readSymmetricSchemaFromXml() {
        Database db = super.readSymmetricSchemaFromXml();
        if (parameterService.is(ParameterConstants.MSSQL_USE_NTYPES_FOR_SYNC)) {
            Table table = db.findTable(TableConstants.getTableName(getTablePrefix(),
                    TableConstants.SYM_DATA));
            setColumnToNtext(table.getColumnWithName("row_data"));
            setColumnToNtext(table.getColumnWithName("old_data"));
            setColumnToNtext(table.getColumnWithName("pk_data"));
        }
        return db;
    } 
    
    protected void setColumnToNtext(Column column) {
        column.setMappedType(TypeMap.LONGNVARCHAR);
    }
    
    @Override
    public boolean createOrAlterTablesIfNecessary(String... tableNames) {
        boolean altered = super.createOrAlterTablesIfNecessary(tableNames);
        altered |= alterLockEscalation();
        return altered;
    }
    
    protected boolean alterLockEscalation () {
        ISqlTemplate sqlTemplate = platform.getSqlTemplate();        
        String tablePrefix = getTablePrefix();
        try {
        	String lockEscalationClause = "";
        	if (parameterService.is(ParameterConstants.MSSQL_LOCK_ESCALATION_DISABLED, true)) {
        		lockEscalationClause = "or t.lock_escalation != 1  or i.allow_page_locks = 'true' ";
        	}
            if (parameterService.is(ParameterConstants.MSSQL_ROW_LEVEL_LOCKS_ONLY, true) && sqlTemplate
                    .queryForInt("select count(*) from sys.indexes i inner join sys.tables t on t.object_id=i.object_id where t.name in ('"
                            + tablePrefix.toLowerCase()
                            + "_outgoing_batch','"
                            + tablePrefix.toLowerCase()
                            + "_data', '"
                            + tablePrefix.toLowerCase()
                            + "_data_event') and (i.allow_row_locks !='true' "
                            + lockEscalationClause
                            + ")") > 0) {
                log.info("Updating indexes to prevent lock escalation");
                
                String dataTable = platform.alterCaseToMatchDatabaseDefaultCase(tablePrefix + "_data");
                String dataEventTable = platform.alterCaseToMatchDatabaseDefaultCase(tablePrefix + "_data_event");
                String outgoingBatchTable = platform.alterCaseToMatchDatabaseDefaultCase(tablePrefix + "_outgoing_batch");
                
                sqlTemplate.update("ALTER INDEX ALL ON " + dataTable
                        + " SET (ALLOW_ROW_LOCKS = ON)");
                sqlTemplate.update("ALTER INDEX ALL ON " + dataEventTable
                        + " SET (ALLOW_ROW_LOCKS = ON)");
                sqlTemplate.update("ALTER INDEX ALL ON " + outgoingBatchTable
                        + " SET (ALLOW_ROW_LOCKS = ON)");
                
                if (parameterService.is(ParameterConstants.MSSQL_LOCK_ESCALATION_DISABLED, true)) {
	                sqlTemplate.update("ALTER INDEX ALL ON " + dataTable
	                        + " SET (ALLOW_PAGE_LOCKS = OFF)");
	                sqlTemplate.update("ALTER INDEX ALL ON " + dataEventTable
	                        + " SET (ALLOW_PAGE_LOCKS = OFF)");
	                sqlTemplate.update("ALTER INDEX ALL ON " + outgoingBatchTable
	                        + " SET (ALLOW_PAGE_LOCKS = OFF)");
	              
	                sqlTemplate.update("ALTER TABLE " + dataTable
	                        + " SET (LOCK_ESCALATION = DISABLE)");
	                sqlTemplate.update("ALTER TABLE " + dataEventTable
	                        + " SET (LOCK_ESCALATION = DISABLE)");
	                sqlTemplate.update("ALTER TABLE " + outgoingBatchTable
	                        + " SET (LOCK_ESCALATION = DISABLE)");
                }
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {            
            log.warn("Failed to disable lock escalation");
            log.debug("", e);
            return false;
        }
    }
    
    @Override
    public void verifyDatabaseIsCompatible() {
        super.verifyDatabaseIsCompatible();
        ISqlTemplate template = getPlatform().getSqlTemplate();
        if (template.queryForInt("select case when (512 & @@options) = 512 then 1 else 0 end") == 1) {
            throw new SymmetricException("NOCOUNT is currently turned ON.  SymmetricDS will not function with NOCOUNT turned ON.");
        }
    }
    
    @Override
    public void createRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base64_encode";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "create function dbo.$(functionName)(@data varbinary(max)) returns varchar(max)                                                                                                                         " + 
                    "\n  with schemabinding, returns null on null input                                                                                                                       " + 
                    "\n  begin                                                                                                                                                                " + 
                    "\n    return ( select [text()] = @data for xml path('') )                                                                                                                " + 
                    "\n  end                                                                                                                                                                  ";
            install(sql, encode);
        }

        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = "create function dbo.$(functionName)() returns smallint                                                                                                                                                 " + 
                    "\n  begin                                                                                                                                                                  " + 
                    "\n    declare @disabled varchar(1);                                                                                                                                        " + 
                    "\n    set @disabled = coalesce(replace(substring(cast(context_info() as varchar), 1, 1), 0x0, ''), '');                                                                    " + 
                    "\n    if @disabled is null or @disabled != '1'                                                                                                                             " + 
                    "\n      return 0;                                                                                                                                                          " + 
                    "\n    return 1;                                                                                                                                                            " + 
                    "\n  end                                                                                                                                                                    ";
            install(sql, triggersDisabled);
        }

        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "create function dbo.$(functionName)() returns varchar(50)                                                                                                                                              " + 
                    "\n  begin                                                                                                                                                                  " + 
                    "\n    declare @node varchar(50);                                                                                                                                           " + 
                    "\n    set @node = coalesce(replace(substring(cast(context_info() as varchar) collate SQL_Latin1_General_CP1_CI_AS, 2, 50), 0x0, ''), '');                                  " + 
                    "\n    return @node;                                                                                                                                                        " + 
                    "\n  end                                                                                                                                                                    ";
            install(sql, nodeDisabled);
        }
        
    }
    
    @Override
    public void dropRequiredDatabaseObjects() {
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

    }

    protected boolean supportsDisableTriggers() {
        if (supportsDisableTriggers == null) {
            try {
                getPlatform().getSqlTemplate().update("set context_info 0x0");
                log.info("This database DOES support disabling triggers during a symmetricds data load");
                supportsDisableTriggers = true;
            } catch (Exception ex) {
                log.warn("This database does NOT support disabling triggers during a symmetricds data load");
                supportsDisableTriggers = false;
            }
        }

        return supportsDisableTriggers == null ? false : supportsDisableTriggers;

    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, final String catalogName, String schemaName,
            final String triggerName, String tableName) {
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
            String previousCatalog = null;
            try {
                previousCatalog = c.getCatalog();
                c.setCatalog(catalog);
                return previousCatalog;
            } catch (SQLException e) {
                if (catalog != null) {
                    try {
                        c.setCatalog(previousCatalog);
                    } catch (SQLException ex) {
                    }
                }
                throw new SqlException(e);
            } 
        } else {
            return null;
        }
    }

    @Override
    protected void postCreateTrigger(ISqlTransaction transaction, StringBuilder sqlBuffer, DataEventType dml,
            Trigger trigger, TriggerHistory hist, Channel channel, String tablePrefix, Table table) {
        if (parameterService.is(ParameterConstants.MSSQL_TRIGGER_ORDER_FIRST, false)) {
            String schemaName = "";
            if (StringUtils.isNotBlank(trigger.getSourceSchemaName())) {
                schemaName = trigger.getSourceSchemaName() + ".";
            }
    
            String triggerNameFirst = (String) transaction.queryForObject(
                    "select tr.name " + 
                    "from sys.triggers tr inner join sys.trigger_events te on te.object_id = tr.object_id " +
                    "inner join sys.tables t on t.object_id = tr.parent_id " +
                    "where t.name = ? and te.type_desc = ? and te.is_first = 1", String.class, table.getName(), dml.name());
            if (StringUtils.isNotBlank(triggerNameFirst)) {
                log.warn("Existing first trigger '{}{}' is being set to order of 'None'", schemaName, triggerNameFirst);
                transaction.execute("exec sys.sp_settriggerorder @triggername = '" + schemaName +
                        triggerNameFirst + "', @order = 'None', @stmttype = '" + dml.name() + "'");            
            }
    
            String triggerName = null;
            if (dml.equals(DataEventType.INSERT)) {
                triggerName = hist.getNameForInsertTrigger();
            } else if (dml.equals(DataEventType.UPDATE)) {
                triggerName = hist.getNameForUpdateTrigger();
            } else {
                triggerName = hist.getNameForDeleteTrigger();
            }
            transaction.execute("exec sys.sp_settriggerorder @triggername = '" + schemaName +
                    triggerName + "', @order = 'First', @stmttype = '" + dml.name() + "'");
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
        if (supportsDisableTriggers()) {
            if (nodeId == null) {
                nodeId = "";
            }
            transaction.prepareAndExecute("DECLARE @CI VarBinary(128);" + "SET @CI=cast ('1"
                    + nodeId + "' as varbinary(128));" + "SET context_info @CI;");
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        if (supportsDisableTriggers()) {
            transaction.prepareAndExecute("set context_info 0x0");
        }
    }

    public String getSyncTriggersExpression() {
    	String catalog = parameterService.is(ParameterConstants.MSSQL_INCLUDE_CATALOG_IN_TRIGGERS, true) ? "$(defaultCatalog)" : "";
        return catalog + "dbo." + parameterService.getTablePrefix()
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
    public void cleanDatabase() {
    }

    @Override
    public boolean needsToSelectLobData() {
        return true;
    }
    
    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "@OldDataRow is null or @DataRow != @OldDataRow";
    }

}
