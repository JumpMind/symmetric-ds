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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class DerbyDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(DerbyDbDialect.class);

    @Override
    protected void initForSpecificDialect() {
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from sys.systriggers where triggername = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.queryForInt("values fn_sym_sync_triggers_set_disabled(1)");
        if (nodeId != null) {
            jdbcTemplate.queryForObject("values fn_sym_sync_node_set_disabled('" + nodeId + "')",
                    String.class);
        }
    }

    public void enableSyncTriggers() {
        jdbcTemplate.queryForInt("values fn_sym_sync_triggers_set_disabled(0)");
        jdbcTemplate.queryForObject("values fn_sym_sync_node_set_disabled(null)", String.class);
    }

    public String getSyncTriggersExpression() {
        return "fn_sym_sync_triggers_disabled() = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "fn_sym_transaction_id()";
    }
    
    @Override
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

    @Override
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            defaultSchema = (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA", String.class);
        }
        return defaultSchema;
    }
}
