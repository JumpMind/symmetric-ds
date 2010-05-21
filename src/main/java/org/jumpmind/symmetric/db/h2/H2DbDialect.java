/*
 * SymmetricDS is an open source database synchronization solution.
 *
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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
package org.jumpmind.symmetric.db.h2;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractEmbeddedDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class H2DbDialect extends AbstractEmbeddedDbDialect implements IDbDialect {

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName, String tableName,
            String triggerName) {
        boolean exists = (jdbcTemplate
                .queryForInt("select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ?",
                        new Object[] { triggerName }) > 0)
                && (jdbcTemplate.queryForInt("select count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        new Object[] { String.format("%s_CONFIG", triggerName) }) > 0);

        if (!exists) {
            removeTrigger(new StringBuilder(), catalogName, schemaName, triggerName, tableName, null);
        }
        return exists;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        final String dropSql = String.format("DROP TRIGGER IF EXISTS %s", triggerName);
        logSql(dropSql, sqlBuffer);

        final String dropTable = String.format("DROP TABLE IF EXISTS %s_CONFIG", triggerName);
        logSql(dropTable, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                int count = jdbcTemplate.update(dropSql);
                if (count > 0) {
                    log.info("TriggerDropped", triggerName);
                }
                count = jdbcTemplate.update(dropTable);
                if (count > 0) {
                    log.info("TableDropped", triggerName);
                }
            } catch (Exception e) {
                log.warn("TriggerDropError", triggerName, e.getMessage());
            }
        }
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.update("set @sync_prevented=1");
        jdbcTemplate.update("set @node_value=?", new Object[] { nodeId });
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set @sync_prevented=null");
        jdbcTemplate.update("set @node_value=null");
    }

    public String getSyncTriggersExpression() {
        return " @sync_prevented is null ";
    }

    /**
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "TRANSACTION_ID()";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public boolean isCharSpacePadded() {
        return false;
    }

    public boolean isCharSpaceTrimmed() {
        return true;
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
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }
}
