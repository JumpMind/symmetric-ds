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

import java.util.Date;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.hsqldb.Token;
import org.hsqldb.types.Binary;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;

public class HsqlDbTrigger extends AbstractEmbeddedTrigger implements org.hsqldb.Trigger {

    static final ILog log = LogFactory.getLog(HsqlDbTrigger.class);

    String triggerName;

    String dataSelectSql;

    String transactionIdSql;

    boolean conditionalExists;

    static String transactionId;

    static long lastTransactionIdUpdate;

    boolean initialized = false;

    public void fire(int type, String triggerName, String tableName, Object[] oldRow, Object[] newRow) {
        try {
            init(type, triggerName, tableName);
            if (initialized) {
                HsqlDbDialect dialect = getDbDialect();
                if (trigger.isSyncOnIncomingBatch() || dialect.isSyncEnabled() && isInsertDataEvent(oldRow, newRow)) {
                    Data data = createData(oldRow, newRow);
                    dataService.insertData(data);

                }
            }
        } catch (RuntimeException ex) {
            log.error(ex);
            throw ex;
        } finally {
            lastTransactionIdUpdate = System.currentTimeMillis();
        }
    }

    protected Data createData(Object[] oldRow, Object[] newRow) {
        Data data = new Data(StringUtils.isBlank(trigger.getTargetTableName()) ? tableName : trigger
                .getTargetTableName(), triggerType, formatRowData(oldRow, newRow), formatPkRowData(oldRow, newRow),
                triggerHistory, trigger.getChannelId(), getTransactionId(oldRow, newRow), getDbDialect()
                        .getSyncNodeDisabled());
        if (triggerType == DataEventType.UPDATE && trigger.isSyncColumnLevel()) {
            data.setOldData(formatAsCsv(getOrderedColumnValues(oldRow)));
        }

        return data;
    }

    private boolean isInsertDataEvent(Object[] oldRow, Object[] newRow) {
        if (conditionalExists) {
            return getDbDialect().getJdbcTemplate().queryForInt(fillVirtualTableSql(dataSelectSql, oldRow, newRow)) > 0;
        } else {
            return true;
        }
    }

    private void init(int type, String triggerName, String tableName) {
        if (!initialized) {
            this.triggerName = triggerName;
            if (initialize(getDataEventType(type), tableName)) {
                buildDataSelectSql();
                buildTransactionIdSql();
                log.debug("TriggerInitializing", triggerName, triggerType);
                initialized = true;
            }
        }
    }

    private String buildVirtualTableSql() {
        StringBuilder b = new StringBuilder("(select ");
        for (String column : includedColumns) {
            b.append("? as ");
            if (Token.isKeyword(column) || column.indexOf(" ") != -1) {
                b.append("\"new_").append(column).append("\",");
            } else {
                b.append("new_").append(column).append(",");
            }
        }

        for (String column : includedColumns) {
            b.append("? as ");
            if (Token.isKeyword(column) || column.indexOf(" ") != -1) {
                b.append("\"old_").append(column).append("\",");
            } else {
                b.append("old_").append(column).append(",");
            }
        }
        b.deleteCharAt(b.length() - 1);
        b.append(" from " + HsqlDbDialect.DUAL_TABLE + ") t ");
        return b.toString();
    }

    private void buildTransactionIdSql() {
        if (!StringUtils.isBlank(trigger.getTxIdExpression())) {
            transactionIdSql = "select " + replaceOldNewTriggerTokens(trigger.getTxIdExpression()) + " from "
                    + buildVirtualTableSql();
        }
    }

    /**
     * I wanted to do this as a preparedstatement but hsqldb doesn't seem to
     * support it.
     */
    private String fillVirtualTableSql(String sql, Object[] oldRow, Object[] newRow) {
        Object[] values = ArrayUtils.addAll(getOrderedColumnValues(newRow), getOrderedColumnValues(oldRow));
        StringBuilder out = new StringBuilder();
        String[] tokens = StringUtils.split(sql, "?");
        for (int i = 0; i < tokens.length; i++) {
            out.append(tokens[i]);
            if (i < values.length) {
                Object value = values[i];
                if (value instanceof String) {
                    out.append("'");
                    out.append(escapeString(value));
                    out.append("'");
                } else if (value instanceof Number) {
                    out.append(value);
                } else if (value instanceof Date) {
                    out.append("'");
                    out.append(AbstractEmbeddedTrigger.dateFormatter.format(value));
                    out.append("'");
                } else {
                    // anything else is unsupported
                    out.append("null");
                }
            }
        }
        return out.toString();
    }

    protected String escapeString(Object val) {
        return val == null ? null : val.toString().replaceAll("'", "''");
    }

    private void buildDataSelectSql() {
        StringBuilder b = new StringBuilder("select count(*) from ");
        b.append(buildVirtualTableSql());
        switch (triggerType) {
        case INSERT:
            if (!StringUtils.isBlank(trigger.getSyncOnInsertCondition())) {
                conditionalExists = true;
                b.append("where ");
                b.append(trigger.getSyncOnInsertCondition());
            }
            break;
        case UPDATE:
            if (!StringUtils.isBlank(trigger.getSyncOnUpdateCondition())) {
                conditionalExists = true;
                b.append("where ");
                b.append(trigger.getSyncOnUpdateCondition());
            }
            break;
        case DELETE:
            if (!StringUtils.isBlank(trigger.getSyncOnDeleteCondition())) {
                conditionalExists = true;
                b.append("where ");
                b.append(trigger.getSyncOnDeleteCondition());
            }
            break;
        }

        this.dataSelectSql = replaceOldNewTriggerTokens(b.toString());
    }

    private String replaceOldNewTriggerTokens(String targetString) {
        // This is a little hack to allow us to replace not only the old/new
        // alias's, but also the column prefix for
        // use in a virtual table we can match SQL expressions against.
        targetString = StringUtils.replace(targetString, "$(newTriggerValue).", "$(newTriggerValue)");
        targetString = StringUtils.replace(targetString, "$(oldTriggerValue).", "$(oldTriggerValue)");
        targetString = StringUtils.replace(targetString, "$(curTriggerValue).", "$(curTriggerValue)");
        return dbDialect.replaceTemplateVariables(triggerType, trigger, triggerHistory, targetString);
    }

    private DataEventType getDataEventType(int type) {
        switch (type) {
        case org.hsqldb.Trigger.INSERT_AFTER_ROW:
            return DataEventType.INSERT;
        case org.hsqldb.Trigger.UPDATE_AFTER_ROW:
            return DataEventType.UPDATE;
        case org.hsqldb.Trigger.DELETE_AFTER_ROW:
            return DataEventType.DELETE;
        default:
            throw new IllegalStateException("Unexpected trigger type: " + type);
        }
    }

    private HsqlDbDialect getDbDialect() {
        return (HsqlDbDialect) dbDialect;
    }

    @Override
    protected String getEngineName() {
        String minusTriggerId = triggerName.substring(0, triggerName.lastIndexOf("_"));
        return minusTriggerId.substring(minusTriggerId.lastIndexOf("_") + 1);
    }

    @Override
    protected int getTriggerHistId() {
        return Integer.parseInt(triggerName.substring(triggerName.lastIndexOf("_") + 1));
    }

    @Override
    protected boolean toCsv(Object object, StringBuilder b) {
        boolean handled = true;
        if (object instanceof Binary) {
            b.append("\"");
            Binary d = (Binary) object;
            b.append(new String(Base64.encodeBase64(d.getBytes())));
            b.append("\"");
        } else {
            handled = super.toCsv(object, b);
        }
        return handled;
    }

    protected String getTransactionId(Object[] oldRow, Object[] newRow) {
        if (System.currentTimeMillis() - lastTransactionIdUpdate > 5000) {
            transactionId = RandomStringUtils.randomAlphanumeric(12);
        }

        if (transactionIdSql != null) {
            transactionId = (String) getDbDialect().getJdbcTemplate().queryForObject(
                    fillVirtualTableSql(transactionIdSql, oldRow, newRow), String.class);
        }
        return transactionId;
    }
}
