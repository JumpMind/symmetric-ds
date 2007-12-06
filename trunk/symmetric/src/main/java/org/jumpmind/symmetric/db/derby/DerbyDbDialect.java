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

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "";

    protected void initForSpecificDialect() {
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        return true;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String schema, String tableName, String triggerName) {
        return true;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        throw new RuntimeException("Not implemented.  Use removeTrigger(schema, trigger, table) instead.");
    }
    
    public void removeTrigger(String schemaName, String triggerName, String tableName) {
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

    public void purge() {
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA",
                String.class);
    }
}
