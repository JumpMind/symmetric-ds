/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Eric Weimer <eweimer@users.sourceforge.net>
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

package org.jumpmind.symmetric.db.db2;

import java.net.URL;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;

public class Db2v9DbDialect extends Db2DbDialect implements IDbDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        try {
            enableSyncTriggers();
        } catch (Exception e) {
            try {
                log.info("EnvironmentVariablesCreating", SYNC_TRIGGERS_DISABLED_USER_VARIABLE,
                        SYNC_TRIGGERS_DISABLED_NODE_VARIABLE);
                new SqlScript(getSqlScriptUrl(), getPlatform().getDataSource(), ";").execute();
            } catch (Exception ex) {
                log.error("DB2DialectInitializingError", ex);
            }
        }
    }
    
    private URL getSqlScriptUrl() {
        return getClass().getResource("/org/jumpmind/symmetric/db/db2.sql");
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    public String getSourceNodeExpression() {
        return SYNC_TRIGGERS_DISABLED_NODE_VARIABLE;
    }
    
}
