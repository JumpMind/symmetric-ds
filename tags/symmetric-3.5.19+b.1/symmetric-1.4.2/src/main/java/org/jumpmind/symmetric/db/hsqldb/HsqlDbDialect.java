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
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class HsqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(HsqlDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "";

    public static String DUAL_TABLE = "DUAL";

    private boolean initializeDatabase;

    private boolean charFieldTrimmed = false;

    private static boolean hsqldbInitialized = false;

    ThreadLocal<Boolean> syncEnabled = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.TRUE;
        }

    };

    protected void initForSpecificDialect() {
        if (initializeDatabase) {
            if (!hsqldbInitialized) {
                jdbcTemplate.update("SET WRITE_DELAY 100 MILLIS");
                jdbcTemplate.update("SET PROPERTY \"hsqldb.default_table_type\" 'cached'");
                jdbcTemplate.update("SET PROPERTY \"sql.enforce_strict_size\" true");
                Runtime.getRuntime().addShutdownHook(new Thread() {

                    @Override
                    public void run() {
                        jdbcTemplate.update("SHUTDOWN");
                    }
                });
                hsqldbInitialized = true;
            }
        }

        createDummyDualTable();

        charFieldTrimmed = jdbcTemplate
                .queryForInt("select count(*) from INFORMATION_SCHEMA.SYSTEM_PROPERTIES where property_name='sql.enforce_strict_size' and property_value='true'") == 1;

        if (jdbcTemplate
                .queryForInt("select count(*) from INFORMATION_SCHEMA.SYSTEM_ALIASES where ALIAS='BASE64_ENCODE'") == 0) {
            jdbcTemplate
                    .update("CREATE ALIAS BASE64_ENCODE for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.encodeBase64\"");
        }
    }

    /**
     * This is for use in the java triggers so we can create a virtual table w/
     * old and new columns values to bump SQL expressions up against.
     */
    private void createDummyDualTable() {
        Table table = getMetaDataFor(null, null, DUAL_TABLE, false);
        if (table == null) {
            jdbcTemplate.update("CREATE MEMORY TABLE " + DUAL_TABLE + "(DUMMY VARCHAR(1))");
            jdbcTemplate.update("INSERT INTO " + DUAL_TABLE + " VALUES(NULL)");
            jdbcTemplate.update("SET TABLE " + DUAL_TABLE + " READONLY TRUE");
        }

    }

    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt(
                "select count(*) from INFORMATION_SCHEMA.SYSTEM_TRIGGERS where trigger_name = ?",
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

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        removeTrigger(schemaName, triggerName);
    }

    public boolean isBlobSyncSupported() {
        return true;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public boolean isSyncEnabled() {
        return syncEnabled.get();
    }

    public void disableSyncTriggers() {
        syncEnabled.set(Boolean.FALSE);
    }

    public void enableSyncTriggers() {
        syncEnabled.set(Boolean.TRUE);
    }

    public String getSyncTriggersExpression() {
        return "1 = 1";
    }

    /**
     * This is not used by the HSQLDB Java triggers
     */
    public String getTransactionTriggerExpression(Trigger trigger) {
        return "not used";
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "call IDENTITY()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public boolean isCharSpacePadded() {
        return charFieldTrimmed;
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

    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return null;
    }

    public void setInitializeDatabase(boolean initializeDatabase) {
        this.initializeDatabase = initializeDatabase;
    }
}
