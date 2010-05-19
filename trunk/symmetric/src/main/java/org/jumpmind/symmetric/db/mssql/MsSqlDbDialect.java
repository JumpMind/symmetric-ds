/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.AutoIncrementColumnFilter;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

/**
 * This dialect was tested with the jTDS JDBC driver on SQL Server 2005.
 * 
 * TODO support text and image fields, they cannot be referenced from the
 * inserted or deleted tables in the triggers. Here is one idea we could
 * implement: http://www.devx.com/getHelpOn/10MinuteSolution/16544
 */
public class MsSqlDbDialect extends AbstractDbDialect implements IDbDialect {
    
    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected Integer overrideJdbcTypeForColumn(Map values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.startsWith("TEXT")) {
            return Types.CLOB;          
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
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
    public void prepareTableForDataLoad(Table table) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            jdbcTemplate.execute("SET IDENTITY_INSERT " + table.getName() + " ON");
        }
    }

    @Override
    public void cleanupAfterDataLoad(Table table) {
        if (table != null && table.getAutoIncrementColumns().length > 0) {
            jdbcTemplate.execute("SET IDENTITY_INSERT " + table.getName() + " OFF");
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

    public void disableSyncTriggers(String nodeId) {
        if (nodeId == null) {
            nodeId = "";
        }
        jdbcTemplate.update("DECLARE @CI VarBinary(128);" + "SET @CI=cast ('1" + nodeId + "' as varbinary(128));"
                + "SET context_info @CI;");
    }

    public void enableSyncTriggers() {
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

    /**
     * SQL Server always pads character fields out to the right to fill out
     * field with space characters.
     * 
     * @return true always
     */
    public boolean isCharSpacePadded() {
        return true;
    }

    /**
     * @return false always
     */
    public boolean isCharSpaceTrimmed() {
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

    /**
     * SQL Server pads an empty string with spaces.
     * 
     * @return false always
     */
    public boolean isEmptyStringNulled() {
        return false;
    }

    /**
     * Nothing to do for SQL Server
     */
    public void purge() {
    }

    public String getDefaultCatalog() {
        return (String) jdbcTemplate.queryForObject("select DB_NAME()", String.class);
    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) jdbcTemplate.queryForObject("select SCHEMA_NAME()", String.class);
        }
        return this.defaultSchema;
    }

    @Override
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public boolean needsToSelectLobData() {
        return true;
    }

}
