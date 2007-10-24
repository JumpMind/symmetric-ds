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

package org.jumpmind.symmetric.db.mysql;

import java.net.URL;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;

public class MySqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(MySqlDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    protected void initForSpecificDialect() {
        try {
            if (!isFunctionUpToDate(TRANSACTION_ID_FUNCTION_NAME)) {
                logger
                        .info("Creating function "
                                + TRANSACTION_ID_FUNCTION_NAME);
                new SqlScript(getTransactionIdSqlUrl(), getPlatform()
                        .getDataSource(), '/').execute();
            }
        } catch (Exception ex) {
            logger.error("Error while initializing MySql.", ex);
        }
    }

    private URL getTransactionIdSqlUrl() {
        return getClass().getResource("/mysql-transactionid.sql");
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        long lastModified = getTransactionIdSqlUrl().openConnection()
                .getLastModified();
        String checkSchema = (getDefaultSchema() != null && getDefaultSchema()
                .length() > 0) ? " and routine_schema='" + getDefaultSchema()
                + "'" : "";
        return jdbcTemplate
                .queryForInt(
                        "select count(*) from information_schema.routines where created >= ? and routine_name=?"
                                + checkSchema, new Object[] {
                                new Date(lastModified), name }) > 0;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String schema,
            String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null
                : getDefaultSchema()) : schema;
        String checkSchema = (schema != null && schema.length() > 0) ? " and trigger_schema='"
                + schema + "'"
                : "";
        return jdbcTemplate
                .queryForInt(
                        "select count(*) from information_schema.triggers where trigger_name like ? and event_object_table like ?"
                                + checkSchema, new Object[] { triggerName,
                                tableName }) > 0;
    }

    // TODO this belongs in SqlTemplate
    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public void disableSyncTriggers() {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "=1");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                + "=null");
    }

    public String getTransactionTriggerExpression() {
        return getDefaultSchema() + "." + TRANSACTION_ID_FUNCTION_NAME + "()";
    }

    public boolean isCharSpacePadded() {
        return false;
    }

    public boolean isCharSpaceTrimmed() {
        return true;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("select database()",
                String.class);
    }

    /**
     * According to the documentation (and experience) the jdbc driver for mysql requires the 
     * fetch size to be as follows.
     */
    @Override   
    public int getStreamingResultsFetchSize() {
        return Integer.MIN_VALUE;
    }
    
    

}
