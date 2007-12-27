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

package org.jumpmind.symmetric.db.derby;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;

public class DerbyDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(DerbyDbDialect.class);

    protected void initForSpecificDialect() {
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        return true;
    }

    protected boolean doesTriggerExistOnPlatform(String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from sys.systriggers where triggername = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger " + triggerName + " does not exist");
        }
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName + "p");
        } catch (Exception e) {
            logger.warn("Trigger " + triggerName + "p does not exist");
        }
    }
    
    public void removeTrigger(String schemaName, String triggerName, String tableName) {
        removeTrigger(schemaName, triggerName);
    }

    public boolean isBlobSyncSupported() {
        return false;
    }
    
    public boolean isClobSyncSupported() {
        return false;
    }

    public void disableSyncTriggers() {
        jdbcTemplate.execute("values fn_sym_sync_triggers_set_disabled(1)");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.execute("values fn_sym_sync_triggers_set_disabled(0)");
    }

    public String getSyncTriggersExpression() {
        return "fn_sym_sync_triggers_disabled = 1";
    }

    public String getTransactionTriggerExpression() {
        return "fn_sym_transaction_id()";
    }
    
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
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

    public boolean supportsMixedCaseNamesInCatalog() {
        return false;
    }

    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
    
    public void purge() {
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA",
                String.class);
    }
}
