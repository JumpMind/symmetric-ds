/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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

package org.jumpmind.symmetric.db.firebird;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class FirebirdDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(FirebirdDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "explain_pretty_print";

    protected void initForSpecificDialect() {
        /*
         * try { if (!isFunctionUpToDate(TRANSACTION_ID_FUNCTION_NAME)) {
         * logger.info("Creating function " + TRANSACTION_ID_FUNCTION_NAME); new
         * SqlScript(getTransactionIdSqlUrl(), getPlatform().getDataSource(),
         * '/') .execute(); } } catch (Exception e) {
         * 
         * logger.error("Error while initializing PostgreSql.", e); }
         */
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        /*
         * long lastModified =
         * getTransactionIdSqlUrl().openConnection().getLastModified(); String
         * checkSchema = (getDefaultSchema() != null &&
         * getDefaultSchema().length() > 0) ? " and routine_schema = '" +
         * getDefaultSchema() + "'" : ""; return jdbcTemplate.queryForInt(
         * "select count(*) from information_schema.routines where created >= ?
         * and routine_name = ?" + checkSchema, new Object[] { new
         * Date(lastModified), name }) > 0;
         */
        return false;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        throw new RuntimeException("Not implemented.  Use removeTrigger(schema, trigger, table) instead.");
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public void disableSyncTriggers() {
        // jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "
        // to off");
    }

    public void enableSyncTriggers() {
        // jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "
        // to on");
    }

    public String getSyncTriggersExpression() {
        // return "current_setting('" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE +
        // "') = 'on'";
        return "1 = 1";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "null";
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select gen_id(gen_" + sequenceName + ", 1) from rdb$database";
    }

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getName() {
        return super.getName().substring(0, 49);
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return null;
    }
}
