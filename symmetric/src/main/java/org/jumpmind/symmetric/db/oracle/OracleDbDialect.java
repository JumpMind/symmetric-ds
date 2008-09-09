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

package org.jumpmind.symmetric.db.oracle;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.Trigger;

public class OracleDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(OracleDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String PACKAGE = "pack_symmetric";

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    @Override
    protected void initForSpecificDialect() {
        try {
            if (!isPackageUpToDate(PACKAGE)) {
                logger.info("Creating package " + PACKAGE);
                new SqlScript(getSqlScriptUrl(), getPlatform().getDataSource(), '/').execute();
            }
        } catch (Exception ex) {
            logger.error("Error while initializing Oracle.", ex);
        }
    }

    private URL getSqlScriptUrl() {
        return getClass().getResource("/dialects/oracle.sql");
    }

    private boolean isPackageUpToDate(String name) throws Exception {
        return jdbcTemplate.queryForInt("select count(*) from user_objects where object_name= upper(?) ",
                new Object[] { name }) > 0;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return true;
    }

    public boolean isBlobOverrideToBinary() {
        return true;
    }

    public boolean isDateOverrideToTimestamp() {
        return true;
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

    public String getTransactionTriggerExpression(Trigger trigger) {
        return TRANSACTION_ID_FUNCTION_NAME + "()";
    }

    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    protected String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case OUTGOING_BATCH:
            return "SEQ_SYM_OUTGOIN_BATCH_BATCH_ID";
        case DATA:
            return "SEQ_SYM_DATA_DATA_ID";
        case TRIGGER_HIST:
            return "SEQ_SYM_TRIGGER_RIGGER_HIST_ID";
        }
        return null;
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt(
                "select count(*) from user_triggers where trigger_name like upper(?) and table_name like upper(?)",
                new Object[] { triggerName, tableName }) > 0;
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public void purge() {
        jdbcTemplate.update("purge recyclebin");
    }

    public void disableSyncTriggers() {
        jdbcTemplate.update("call pack_symmetric.setValue(1)");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("call pack_symmetric.setValue(null)");
    }

    public String getSyncTriggersExpression() {
        return "fn_trigger_disabled() is null";
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual",
                String.class);
    }

}
