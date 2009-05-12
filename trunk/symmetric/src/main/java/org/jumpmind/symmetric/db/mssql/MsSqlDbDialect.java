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
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.Trigger;

/**
 * This dialect was tested with the jTDS JDBC driver on SQL Server 2005.
 * 
 * TODO support text and image fields, they cannot be referenced from the
 * inserted or deleted tables in the triggers. Here is one idea we could
 * implement: http://www.devx.com/getHelpOn/10MinuteSolution/16544
 */
public class MsSqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(MsSqlDbDialect.class);

    @Override
    protected void initForSpecificDialect() {
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    public IColumnFilter getDatabaseColumnFilter() {
        return new IColumnFilter() {
            int[] indexesToRemove = null;

            public String[] filterColumnsNames(IDataLoaderContext ctx, DmlType dml, Table table, String[] columnNames) {
                ArrayList<String> columns = new ArrayList<String>();
                CollectionUtils.addAll(columns, columnNames);
                if (dml == DmlType.UPDATE) {
                    Column[] autoIncrementColumns = table.getAutoIncrementColumns();
                    indexesToRemove = new int[autoIncrementColumns.length];
                    int i = 0;
                    for (Column column : autoIncrementColumns) {
                        String name = column.getName();
                        int index = columns.indexOf(name);

                        if (index < 0) {
                            name = name.toLowerCase();
                            index = columns.indexOf(name);
                        }
                        if (index < 0) {
                            name = name.toUpperCase();
                            index = columns.indexOf(name);
                        }
                        indexesToRemove[i++] = index;
                        columns.remove(name);
                    }
                }
                return columns.toArray(new String[columns.size()]);
            }

            public Object[] filterColumnsValues(IDataLoaderContext ctx, DmlType dml, Table table, Object[] columnValues) {
                if (dml == DmlType.UPDATE && indexesToRemove != null) {
                    ArrayList<Object> values = new ArrayList<Object>();
                    CollectionUtils.addAll(values, columnValues);
                    for (int index : indexesToRemove) {
                        if (index >= 0) {
                            values.remove(index);
                        }
                    }
                    return values.toArray(new Object[values.size()]);
                }
                return columnValues;
            }

            public boolean isAutoRegister() {
                return false;
            }

        };
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
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from sysobjects where type = 'TR' AND name = ?",
                new Object[] { triggerName }) > 0;
    }

    public void disableSyncTriggers(String nodeId) {
        if (nodeId == null) {
            nodeId = "";
        }
        jdbcTemplate.update("DECLARE @CI VarBinary(128);" + "SET @CI=cast ('1" + nodeId
                + "' as varbinary(128));" + "SET context_info @CI;");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set context_info 0x0");
    }

    public String getSyncTriggersExpression() {
        return "dbo.fn_sym_triggers_disabled() = 0";
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

}
