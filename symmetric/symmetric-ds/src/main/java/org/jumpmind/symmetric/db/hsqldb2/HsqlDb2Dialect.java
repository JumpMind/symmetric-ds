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
package org.jumpmind.symmetric.db.hsqldb2;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class HsqlDb2Dialect extends AbstractDbDialect implements IDbDialect {

    public static String DUAL_TABLE = "DUAL";

    private boolean enforceStrictSize = true;

    boolean dualTableCreated = false;

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName,
            String tableName, String triggerName) {
        boolean exists = (jdbcTemplate.queryForInt(
                "select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ?",
                new Object[] { triggerName }) > 0)
                || (jdbcTemplate.queryForInt(
                        "select count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        new Object[] { String.format("%s_CONFIG", triggerName) }) > 0);
        return exists;
    }
    
    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName,
            String triggerName, String tableName, TriggerHistory oldHistory) {
        final String dropSql = String.format("DROP TRIGGER %s", triggerName);
        logSql(dropSql, sqlBuffer);

        final String dropTable = String.format("DROP TABLE IF EXISTS %s_CONFIG", triggerName);
        logSql(dropTable, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                int count = jdbcTemplate.update(dropSql);
                if (count > 0) {
                    log.info("TriggerDropped", triggerName);
                }
            } catch (Exception e) {
                log.warn("TriggerDropError", triggerName, e.getMessage());
            }
            try {
                int count = jdbcTemplate.update(dropTable);
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
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('sync_prevented','1')");
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('node_value','" + nodeId + "')");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('sync_prevented',null)");
        jdbcTemplate.execute("CALL " + tablePrefix + "_set_session('node_value',null)");
    }

    public String getSyncTriggersExpression() {
        return " " + tablePrefix + "_get_session('sync_prevented') is null ";
    }

    /**
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        // TODO A method is coming that will all access to the transaction id ...
        return "null";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }    
    
    public boolean isCharSpacePadded() {
        return enforceStrictSize;
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
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    public void truncateTable(String tableName) {
        jdbcTemplate.update("delete from " + tableName);
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return (String) jdbcTemplate.queryForObject("select value from INFORMATION_SCHEMA.SYSTEM_SESSIONINFO where key='CURRENT SCHEMA'",
                String.class);
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
    }

    @Override
    public boolean canGapsOccurInCapturedDataIds() {
        return false;
    }
    
}
