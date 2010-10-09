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

import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.support.lob.OracleLobHandler;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class OracleDbDialect extends AbstractDbDialect implements IDbDialect {

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    String selectTriggerSql;
    
    String selectTransactionsSql;

    @Override
    public void init(Platform pf) {
        super.init(pf);
        try {
            areDatabaseTransactionsPendingSince(System.currentTimeMillis());
            supportsTransactionViews = true;
        } catch (Exception ex) {
            if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW)) {
                log.warn(ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void initLobHandler() {
        lobHandler = new OracleLobHandler();
        try {
            Class clazz = Class.forName(parameterService
                    .getString(ParameterConstants.DB_NATIVE_EXTRACTOR));
            NativeJdbcExtractor nativeJdbcExtractor = (NativeJdbcExtractor) clazz.newInstance();
            ((OracleLobHandler) lobHandler).setNativeJdbcExtractor(nativeJdbcExtractor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Integer overrideJdbcTypeForColumn(Map values) {
        String typeName = (String) values.get("TYPE_NAME");
        // This is for Oracle's TIMESTAMP(9)
        if (typeName != null && typeName.startsWith("TIMESTAMP")) {
            return Types.TIMESTAMP;
            // This is for Oracle's NVARCHAR type
        } else if (typeName != null && typeName.startsWith("NVARCHAR")) {
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("NCHAR")) {
            return Types.CHAR;            
        } else if (typeName != null && typeName.startsWith("BINARY_FLOAT")) {
            return Types.FLOAT;
        } else if (typeName != null && typeName.startsWith("BINARY_DOUBLE")) {
            return Types.DOUBLE;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
    }

    @Override
    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, Trigger trigger,
            TriggerHistory hist, String tablePrefix, Table table) {
        try {
            super.createTrigger(sqlBuffer, dml, trigger, hist, tablePrefix, table);
        } catch (BadSqlGrammarException ex) {
            if (ex.getSQLException().getErrorCode() == 4095) {
                try {
                    // a trigger of the same name must already exist on a table
                    log.warn("TriggerAlreadyExists", jdbcTemplate.queryForMap(
                            "select * " + selectTriggerSql,
                            new Object[] { hist.getTriggerNameForDmlType(dml), hist.getSourceTableName() }));
                } catch (DataAccessException e) {
                }
            }
            throw ex;
        }
    }

    @Override
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

    @Override
    public boolean isDateOverrideToTimestamp() {
        return true;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return tablePrefix + "_" + "transaction_id()";
    }

    @Override
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

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return jdbcTemplate
                .queryForInt(
                        "select count(*) " + selectTriggerSql,
                        new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public void purge() {
        jdbcTemplate.update("purge recyclebin");
    }

    protected String getSymmetricPackageName() {
        return tablePrefix + "_pkg";
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.update(String.format("call %s.setValue(1)", getSymmetricPackageName()));
        if (nodeId != null) {
            jdbcTemplate.update(String.format("call %s.setNodeValue('" + nodeId + "')",
                    getSymmetricPackageName()));
        }
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update(String.format("call %s.setValue(null)", getSymmetricPackageName()));
        jdbcTemplate.update(String.format("call %s.setNodeValue(null)", getSymmetricPackageName()));
    }

    public String getSyncTriggersExpression() {
        return tablePrefix + "_trigger_disabled() is null";
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) jdbcTemplate.queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {

    }

    @Override
    public boolean areDatabaseTransactionsPendingSince(long time) {
        String returnValue = jdbcTemplate.queryForObject(selectTransactionsSql, String.class);
        if (returnValue != null) {
            Date date;
            try {
                date = DateUtils.parseDate(returnValue, new String[] { "MM/dd/yy HH:mm:ss" });
                return date.getTime() < time;
            } catch (ParseException e) {
                log.error(e);
                return true;
            }
        } else {
            return false;
        }

    }

    public void setSelectTransactionsSql(String selectTransactionSql) {
        this.selectTransactionsSql = selectTransactionSql;
    }
    
    public void setSelectTriggerSql(String selectTriggerSql) {
        this.selectTriggerSql = selectTriggerSql;
    }
    
    @Override
    public boolean supportsTransactionViews() {
        return supportsTransactionViews && parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW);
    }
    
    @Override
    public String massageDataExtractionSql(String sql, String channelId) {
        String channels = parameterService.getString(ParameterConstants.DBDIALECT_ORACLE_USE_DBMS_LOB_SUBSTR);
        if (!StringUtils.isBlank(channels) && channels.contains(channelId)) {
            sql = StringUtils.replace(sql, "d.row_data", "dbms_lob.substr(d.row_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.old_data", "dbms_lob.substr(d.old_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.pk_data", "dbms_lob.substr(d.pk_data, 4000, 1 )");
        }
        return sql;        
    }
    
}
