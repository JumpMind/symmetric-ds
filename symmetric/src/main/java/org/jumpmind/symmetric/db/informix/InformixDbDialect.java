/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) JumpMind, Inc.
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
package org.jumpmind.symmetric.db.informix;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.AutoIncrementColumnFilter;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Trigger;

public class InformixDbDialect extends AbstractDbDialect implements IDbDialect {

    private String identifierQuoteString = "";
    
    private Map<String, String> sqlScriptReplacementTokens;
    
    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
	Map<String, String> env = System.getenv();
	String clientIdentifierMode = env.get("DELIMIDENT");
	if (clientIdentifierMode != null && clientIdentifierMode.equalsIgnoreCase("y")) {
	    identifierQuoteString = "\"";
	}
	sqlScriptReplacementTokens = new HashMap<String, String>();
	sqlScriptReplacementTokens.put("current_timestamp", "current");
    }

    @Override
    public IColumnFilter getDatabaseColumnFilter() {
	return new AutoIncrementColumnFilter();
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
	    String triggerName) {
	return jdbcTemplate.queryForInt("select count(*) from systriggers where lower(trigname) = ?",
		new Object[] { triggerName.toLowerCase() }) > 0;
    }

    public void disableSyncTriggers(String nodeId) {
	jdbcTemplate.queryForList("select " + tablePrefix + "_triggers_set_disabled('t'), " + tablePrefix
		+ "_node_set_disabled(?) from sysmaster:sysdual", new Object[] { nodeId });
    }

    public void enableSyncTriggers() {
	jdbcTemplate.queryForList("select " + tablePrefix + "_triggers_set_disabled('f'), " + tablePrefix
		+ "_node_set_disabled(null) from sysmaster:sysdual");
    }

    public String getSyncTriggersExpression() {
	return "not $(defaultSchema)" + tablePrefix + "_triggers_disabled()";
    }

    @Override
    public boolean supportsTransactionId() {
	// TODO: write a user-defined routine in C that calls mi_get_transaction_id() 
	return false;
    }

    @Override
    public boolean isTransactionIdOverrideSupported() {
	return false;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
	return "null";
    }

    @Override
    public boolean isBlobSyncSupported() {
	return false;
    }

    @Override
    public boolean isClobSyncSupported() {
	return false;
    }

    @Override
    public boolean allowsNullForIdentityColumn() {
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

    public void purge() {
    }

    public String getDefaultCatalog() {
	return null;
    }

    @Override
    public String getDefaultSchema() {
	return jdbcTemplate.queryForObject("select trim(user) from sysmaster:sysdual", String.class);
    }

    @Override
    public String getIdentifierQuoteString() {
	return identifierQuoteString;
    }
    
    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
	return sqlScriptReplacementTokens;
    }
}
