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

package org.jumpmind.symmetric.db.hsqldb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;

public class HsqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(HsqlDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "";

    protected void initForSpecificDialect() {
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        return true;
    }

    protected boolean doesTriggerExistOnPlatform(String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from INFORMATION_SCHEMA.SYSTEM_TRIGGERS where trigger_name = ?",
                new Object[] { triggerName }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
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
    }

    public void enableSyncTriggers() {
    }

    public String getSyncTriggersExpression() {
        return "1 = 1";
    }

    public String getTransactionTriggerExpression() {
        return "null";
    }
    
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
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
        return null;
    }
}
