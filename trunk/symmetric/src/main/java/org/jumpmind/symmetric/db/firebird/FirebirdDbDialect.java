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
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.jdbc.UncategorizedSQLException;

public class FirebirdDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(FirebirdDbDialect.class);

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";
    
    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    protected void initForSpecificDialect() {
        try {
            jdbcTemplate.queryForInt("select char_length(sym_escape('')) from rdb$database");
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                logger.error("Please install the sym_udf.so/dll to your {firebird_home}/UDF folder.");
            }
            throw new RuntimeException("Function SYM_ESCAPE is not installed", e);
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        throw new RuntimeException("Not implemented.  Use removeTrigger(schema, trigger, table) instead.");
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "',1) from rdb$database");
        if (nodeId != null) {
            jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "','" + nodeId + "') from rdb$database");
        }
    }

    public void enableSyncTriggers() {
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "',null) from rdb$database");
        jdbcTemplate.queryForInt("select rdb$set_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "',null) from rdb$database");
    }

    public String getSyncTriggersExpression() {
        return "rdb$get_context('USER_SESSION','" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "') is null";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "current_transaction||''";
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select gen_id(gen_" + sequenceName + ", 1) from rdb$database";
    }

    public boolean isBlobSyncSupported() {
        return false;
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
