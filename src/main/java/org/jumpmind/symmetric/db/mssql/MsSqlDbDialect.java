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

import java.util.ArrayList;

import org.apache.commons.collections.CollectionUtils;
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

    protected void initForSpecificDialect() {
    }

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
                        // if (values.size() > index) {
                        values.remove(index);
                        // }
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

    public void disableSyncTriggers() {
        jdbcTemplate.update("set context_info 0x1");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set context_info 0x0");
    }

    public String getSyncTriggersExpression() {
        return "@SyncEnabled <> 0x1";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "@TransactionId";
    }

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

    public boolean isTransactionIdOverrideSupported() {
        return false;
    }

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

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("select SCHEMA_NAME()", String.class);
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

}
