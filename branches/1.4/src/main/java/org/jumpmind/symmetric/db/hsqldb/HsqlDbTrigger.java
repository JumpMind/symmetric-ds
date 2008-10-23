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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Token;
import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.springframework.jdbc.core.RowMapper;

public class HsqlDbTrigger extends AbstractEmbeddedTrigger implements org.hsqldb.Trigger {

    static final Log logger = LogFactory.getLog(HsqlDbTrigger.class);

    String triggerName;

    String dataSelectSql;

    String nodeSelectSql;

    String transactionIdSql;

    boolean conditionalExists;

    static String transactionId;

    static long lastTransactionIdUpdate;

    public void fire(int type, String triggerName, String tableName, Object[] oldRow, Object[] newRow) {
        try {
            init(type, triggerName, tableName);
            HsqlDbDialect dialect = getDbDialect();
            if (trigger.isSyncOnIncomingBatch() || dialect.isSyncEnabled() && isInsertDataEvent(oldRow, newRow)) {
                Data data = createData(oldRow, newRow);
                List<Node> nodes = findTargetNodes(oldRow, newRow);
                if (nodes != null) {
                    dataService.insertDataEvent(data, trigger.getChannelId(), getTransactionId(oldRow, newRow), nodes);
                }
            }
        } catch (RuntimeException ex) {
            logger.error(ex, ex);
            throw ex;
        } finally {
            lastTransactionIdUpdate = System.currentTimeMillis();
        }
    }

    @SuppressWarnings("unchecked")
    protected List<Node> findTargetNodes(Object[] oldRow, Object[] newRow) {
        return (List<Node>) getDbDialect().getJdbcTemplate().query(fillVirtualTableSql(nodeSelectSql, oldRow, newRow),
                new RowMapper() {
                    public Object mapRow(ResultSet rs, int index) throws SQLException {
                        Node node = new Node();
                        node.setNodeId(rs.getString(1));
                        return node;
                    }
                });
    }

    private boolean isInsertDataEvent(Object[] oldRow, Object[] newRow) {
        if (conditionalExists) {
            return getDbDialect().getJdbcTemplate().queryForInt(fillVirtualTableSql(dataSelectSql, oldRow, newRow)) > 0;
        } else {
            return true;
        }
    }

    private void init(int type, String triggerName, String tableName) {
        if (this.triggerName == null) {
            this.triggerName = triggerName;
            this.initialize(getDataEventType(type), tableName);
            buildDataSelectSql();
            buildNodeSelectSql();
            buildTransactionIdSql();
            if (logger.isDebugEnabled()) {
                logger.debug("initializing " + triggerName + " for " + triggerType);
            }
        }
    }

    private String buildVirtualTableSql() {
        StringBuilder b = new StringBuilder("(select ");
        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.INSERT) {
            for (String column : includedColumns) {
                b.append("? as ");
                if (Token.isKeyword(column) || column.indexOf(" ") != -1) {
                    b.append("\"new_").append(column).append("\",");
                } else {
                    b.append("new_").append(column).append(",");
                }
            }
        }

        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.DELETE) {
            for (String column : includedColumns) {
                b.append("? as ");
                if (Token.isKeyword(column) || column.indexOf(" ") != -1) {
                    b.append("\"old_").append(column).append("\",");
                } else {
                    b.append("old_").append(column).append(",");
                }
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
        Object[] values = null;
        switch (triggerType) {
        case INSERT:
            values = getOrderedColumnValues(newRow);
            break;
        case UPDATE:
            values = ArrayUtils.addAll(getOrderedColumnValues(newRow), getOrderedColumnValues(oldRow));
            break;
        case DELETE:
            values = getOrderedColumnValues(oldRow);
            break;
        }
        StringBuilder out = new StringBuilder();
        String[] tokens = StringUtils.split(sql, "?");
        for (int i = 0; i < tokens.length; i++) {
            out.append(tokens[i]);
            if (i < values.length) {
                Object value = values[i];
                if (value instanceof String) {
                    out.append("'");
                    out.append(value);
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

    private void buildNodeSelectSql() {
        StringBuilder b = new StringBuilder("select node_id from ");
        b.append(dbDialect.getTablePrefix());
        b.append("_node c, ");
        b.append(buildVirtualTableSql());
        b.append("where c.node_group_id='");
        b.append(trigger.getTargetGroupId());
        b.append("' and c.sync_enabled=1");
        b.append(replaceOldNewTriggerTokens(trigger.getNodeSelect()));
        this.nodeSelectSql = b.toString();
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
