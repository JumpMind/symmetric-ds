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

package org.jumpmind.symmetric.db.mysql;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class MySqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(MySqlDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";
    
    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";

    private boolean supportsTransactionId = false;

    @Override
    protected void initForSpecificDialect() {
        int[] versions = Version.parseVersion(getProductVersion());
        if (getMajorVersion() == 5
                && (getMinorVersion() == 0 || (getMinorVersion() == 1 && versions[2] < 23))) {
            logger.info("Enabling transaction ID support");
            supportsTransactionId = true;
        }
    }
    
    @Override
    public Table findTable(String catalogName, String schemaName, String tblName)
            throws Exception {
        catalogName = StringUtils.isBlank(catalogName) ? StringUtils.isBlank(schemaName) ? getDefaultCatalog() :schemaName : catalogName; 
        return super.findTable(catalogName, schemaName, tblName);
    }

    @Override
    protected void createRequiredFunctions() {
        String[] functions = sqlTemplate.getFunctionsToInstall();
        for (String funcName : functions) {
            if (! funcName.equals("fn_transaction_id") || supportsTransactionId) {
                if (jdbcTemplate.queryForInt(sqlTemplate.getFunctionInstalledSql(funcName)) == 0) {
                    jdbcTemplate.update(sqlTemplate.getFunctionSql(funcName));
                    logger.info("Just installed " + funcName);
                }
            }
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        catalog = catalog == null ? (getDefaultCatalog() == null ? null : getDefaultCatalog()) : catalog;
        String checkCatalogSql = (catalog != null && catalog.length() > 0) ? " and trigger_schema='" + catalog + "'"
                : "";
        return jdbcTemplate.queryForInt(
                "select count(*) from information_schema.triggers where trigger_name like ? and event_object_table like ?"
                        + checkCatalogSql, new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        catalogName = catalogName == null ? "" : (catalogName + ".");
        final String sql = "drop trigger " + catalogName + triggerName;
        logSql(sql, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                jdbcTemplate.update(sql);
            } catch (Exception e) {
                logger.warn("Trigger does not exist");
            }
        }
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
    public String getTransactionTriggerExpression(Trigger trigger) {
        if (supportsTransactionId) {
            return TRANSACTION_ID_FUNCTION_NAME + "()";
        }
        return "null";
    }

    @Override
    public boolean supportsTransactionId() {
        return supportsTransactionId;
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select last_insert_id()";
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

    public void purge() {
    }

    public String getDefaultCatalog() {
        return (String) jdbcTemplate.queryForObject("select database()", String.class);
    }

    @Override
    protected String switchCatalogForTriggerInstall(String catalog, Connection c) throws SQLException {
        if (catalog != null) {
            String previousCatalog = c.getCatalog();
            c.setCatalog(catalog);
            return previousCatalog;
        } else {
            return null;
        }
    }

    /**
     * According to the documentation (and experience) the jdbc driver for mysql
     * requires the fetch size to be as follows.
     */
    @Override
    public int getStreamingResultsFetchSize() {
        return Integer.MIN_VALUE;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
    
    @Override
    public String getIdentifierQuoteString()
    {
        return "`";
    }

}
