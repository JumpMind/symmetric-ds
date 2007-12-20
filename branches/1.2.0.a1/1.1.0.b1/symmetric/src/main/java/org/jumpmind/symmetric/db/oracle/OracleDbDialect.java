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

package org.jumpmind.symmetric.db.oracle;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;

public class OracleDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(OracleDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    static final String SYNC_TRIGGERS_DISABLED_FUNCTION = "fn_trigger_disabled";
    
    @Override
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
            logger.error("Error while initializing Oracle.", ex);
        }
    }

    private URL getTransactionIdSqlUrl() {
        return getClass().getResource("/oracle-transactionid.sql");
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        long lastModified = getTransactionIdSqlUrl().openConnection()
                .getLastModified();

        SimpleDateFormat dateFormat = new SimpleDateFormat(
                "yyyy-MM-dd':'HH:mm:ss");

        return jdbcTemplate
                .queryForInt(
                        "select count(*) from all_objects where timestamp < ? and object_name= upper(?) ",
                        new Object[] {
                                dateFormat.format(new Date(lastModified)), name }) > 0;
    }

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return true;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public String getTransactionTriggerExpression() {
        return TRANSACTION_ID_FUNCTION_NAME + "()";
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String schema, String tableName,
            String triggerName) {
            return jdbcTemplate
                    .queryForInt(
                            "select count(*) from ALL_TRIGGERS  where trigger_name like upper(?) and table_name like upper(?)",
                            new Object[] { triggerName, tableName }) > 0;
    }

    public void purge() {
        jdbcTemplate.update("purge recyclebin");

    }

    public void disableSyncTriggers() {
        jdbcTemplate.update("call pack_symmetric.setValue(1)");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("call pack_symmetric.setValue(null)");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_FUNCTION + "() is null";
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject(
                "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual",
                String.class);
    }

}
