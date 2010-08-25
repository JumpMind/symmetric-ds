/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Mark Hanes <eegeek@users.sourceforge.net>
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

package org.jumpmind.symmetric.db.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class SqLiteDbDialect extends AbstractDbDialect implements IDbDialect {

//    private static final String TRANSACTION_ID = "transaction_id";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        // TODO Auto-generated method stub
        
    }
   
    public String toFormattedTimestamp(java.util.Date time) {
        StringBuilder ts = new StringBuilder("datetime('");
        ts.append(JDBC_TIMESTAMP_FORMATTER.format(time));
        ts.append("')");
        return ts.toString();
    }
    
    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    private final String getTransactionFunctionName() {
        return null;
    }
    
    /* Below here was originally copied from MySqlDbDialect.  May still need tweaking */
   
    @Override
    protected String getPlatformTableName(String catalogName, String schemaName, String tblName) {
        List<String> tableNames = jdbcTemplate.queryForList("select distinct(tbl_name) from sqlite_master where tbl_name like '%" + tblName + "%'", String.class);
        if (tableNames.size() > 0 ) {
            return tableNames.get(0);
        } else {
            return tblName;
        }
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        catalog = catalog == null ? (getDefaultCatalog() == null ? null : getDefaultCatalog()) : catalog;
        String checkCatalogSql = (catalog != null && catalog.length() > 0) ? " and trigger_schema='" + catalog + "'"
                : "";
        return jdbcTemplate.queryForInt(
                "select count(*) from sqlite_master where type='trigger' and name like ? and tbl_name like ?"
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
                log.warn("TriggerDoesNotExist");
            }
        }
    }

    public void disableSyncTriggers(String nodeId) {
   //     jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
    //    if (nodeId != null) {
      //      jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
       // }
    }

    public void enableSyncTriggers() {
//        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
 //       jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }



    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return getTransactionFunctionName() + "()";
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select last_insert_id()";
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return false;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return true;
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
    protected String switchCatalogForTriggerInstall(String catalog, Connection c) throws SQLException {
        if (catalog != null) {
            String previousCatalog = c.getCatalog();
            c.setCatalog(catalog);
            return previousCatalog;
        } else {
            return null;
        }
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
