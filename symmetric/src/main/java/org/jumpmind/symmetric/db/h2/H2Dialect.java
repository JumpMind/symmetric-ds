/*
 * SymmetricDS is an open source database synchronization solution.
 *
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

/**
 * 
 * @author knaas@users.sourceforge.net
 */
public class H2Dialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(H2Dialect.class);
    public static String DUAL_TABLE = "DUAL";
    private boolean initializeDatabase;
    private static boolean h2Initialized = false;
    private boolean storesUpperCaseNames = true;
    
    private ThreadLocal<Boolean> syncEnabled = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.TRUE;
        }
    };
    
    private ThreadLocal<String> syncNodeDisabled = new ThreadLocal<String>() {

        @Override
        protected String initialValue() {
            return null;
        }
    };

    protected void initForSpecificDialect() {
        if (initializeDatabase) {
            if (!h2Initialized) {
                jdbcTemplate.update("SET WRITE_DELAY 100");
                h2Initialized = true;
            }
        }

        createDummyDualTable();

        jdbcTemplate
                .update("CREATE ALIAS IF NOT EXISTS BASE64_ENCODE for \"org.jumpmind.symmetric.db.h2.H2Functions.encodeBase64\"");
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
            jdbcTemplate.update("REVOKE ALL ON " + DUAL_TABLE + " FROM PUBLIC");
            jdbcTemplate.update("GRANT SELECT ON " + DUAL_TABLE + " TO PUBLIC");
        }

    }

    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from INFORMATION_SCHEMA.TRIGGERS where trigger_name = ?",
                new Object[] { triggerName }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName, TriggerHistory hist) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        triggerName = schemaName + triggerName;
        try {
            jdbcTemplate.update(new String("drop trigger " + triggerName + "_" + getEngineName() + "_"
                    + hist.getTriggerHistoryId()).toUpperCase());
        } catch (Exception e) {
            logger.warn("Error removing " + triggerName + ": " + e.getMessage());
        }
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName,
            TriggerHistory oldHistory) {
        removeTrigger(schemaName, triggerName, oldHistory);
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    public boolean isSyncEnabled() {
        return syncEnabled.get();
    }

    public String getSyncNodeDisabled() {
        return syncNodeDisabled.get();
    }

    public void disableSyncTriggers(String nodeId) {
        syncEnabled.set(Boolean.FALSE);
        syncNodeDisabled.set(nodeId);
    }

    public void enableSyncTriggers() {
        syncEnabled.set(Boolean.TRUE);
        syncNodeDisabled.set(null);
    }

    public String getSyncTriggersExpression() {
        return "1 = 1";
    }

    /**
     * This is not used by the H2 Java triggers
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
        return false;
    }

    public boolean isCharSpaceTrimmed() {
        return true;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return storesUpperCaseNames;
    }

    public boolean supportsGetGeneratedKeys() {
        return false;
    }    
    
    @Override
    public boolean supportsTransactionId() {
        return true;
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
