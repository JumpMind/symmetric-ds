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

import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.AutoIncrementColumnFilter;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.model.Trigger;

public class InformixDbDialect extends AbstractDbDialect implements IDbDialect {

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }
    
    @Override
    public IColumnFilter getDatabaseColumnFilter() {
        return new AutoIncrementColumnFilter();
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from systriggers where trigname = ?",
		new Object[] { triggerName }) > 0;
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.queryForList("select " + tablePrefix
		+ "_triggers_set_disabled('t'), " + tablePrefix
		+ "_node_set_disabled(?) from sysmaster:sysdual",
		new Object[] { nodeId });
    }

    public void enableSyncTriggers() {
        jdbcTemplate.queryForList("select " + tablePrefix
		+ "_triggers_set_disabled('f'), " + tablePrefix
		+ "_node_set_disabled(null) from sysmaster:sysdual");
    }

    public String getSyncTriggersExpression() {
	return "$(defaultSchema)" + tablePrefix + "_triggers_disabled()";
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "null";
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
	return (String) jdbcTemplate.queryForObject("select trim(user) from sysmaster:sysdual", String.class);
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "";
    }
}
