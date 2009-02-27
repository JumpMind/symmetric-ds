/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *               Keith Naas <knaas@users.sourceforge.net>
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.jumpmind.symmetric.db.AbstractEmbeddedTrigger;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.springframework.jdbc.core.RowMapper;

public class H2Trigger extends AbstractEmbeddedTrigger implements org.h2.api.Trigger {

    static final Log logger = LogFactory.getLog(H2Trigger.class);
    protected String triggerName;
    protected String schemaName;
    protected String dataSelectSql;
    protected String nodeSelectSql;
    protected String transactionIdSql;
    protected boolean conditionalExists;
    protected boolean initialized = false;
    protected Set<String> keywords;

    public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        try {
            if (initialized) {
                H2Dialect dialect = getDbDialect();
                if (trigger.isSyncOnIncomingBatch() || dialect.isSyncEnabled() && isInsertDataEvent(oldRow, newRow)) {
                    Data data = createData(oldRow, newRow);
                    List<Node> nodes = findTargetNodes(oldRow, newRow);
                    String disabledNodeId = dialect.getSyncNodeDisabled();
                    if (disabledNodeId != null) {
                        Node disabledNode = new Node();
                        disabledNode.setNodeId(disabledNodeId);
                        nodes.remove(disabledNode);
                    }
                    if (nodes != null) {
                        dataService.insertDataEvent(data, trigger.getChannelId(),
                                getTransactionId(conn, oldRow, newRow), nodes);
                    }
                }
            }
        } catch (RuntimeException ex) {
            logger.error(ex, ex);
            throw ex;
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

    protected boolean isInsertDataEvent(Object[] oldRow, Object[] newRow) {
        if (conditionalExists) {
            return getDbDialect().getJdbcTemplate().queryForInt(fillVirtualTableSql(dataSelectSql, oldRow, newRow)) > 0;
        } else {
            return true;
        }
    }

    protected void initializeMetadata(Connection conn) {
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            keywords = new HashSet<String>(Arrays.asList(metaData.getSQLKeywords().split(",")));
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to fetch keywords", ex);
        }
    }

    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) {
        if (!initialized) {
            this.schemaName = schemaName;
            this.triggerName = triggerName;
            initializeMetadata(conn);
            if (initialize(getDataEventType(type), tableName)) {
                buildDataSelectSql();
                buildNodeSelectSql();
                buildTransactionIdSql();
                if (logger.isDebugEnabled()) {
                    logger.debug("initializing " + this.triggerName + " for " + triggerType);
                }
                initialized = true;
            }
        }
    }

    protected String buildVirtualTableSql() {
        StringBuilder b = new StringBuilder("(select ");
        for (String column : includedColumns) {
            b.append("? as ");
            if (keywords.contains(column) || column.indexOf(" ") != -1) {
                b.append("\"new_").append(column).append("\",");
            } else {
                b.append("new_").append(column).append(",");
            }
        }

        for (String column : includedColumns) {
            b.append("? as ");
            if (keywords.contains(column) || column.indexOf(" ") != -1) {
                b.append("\"old_").append(column).append("\",");
            } else {
                b.append("old_").append(column).append(",");
            }
        }
        b.deleteCharAt(b.length() - 1);
        b.append(" from " + H2Dialect.DUAL_TABLE + ") t ");
        return b.toString();
    }

    protected void buildTransactionIdSql() {
        if (!StringUtils.isBlank(trigger.getTxIdExpression())) {
            transactionIdSql = "select " + replaceOldNewTriggerTokens(trigger.getTxIdExpression()) + " from "
                    + buildVirtualTableSql();
        }
    }

    /**
     * I wanted to do this as a preparedstatement but h2 doesn't seem to support
     * it???
     */
    protected String fillVirtualTableSql(String sql, Object[] oldRow, Object[] newRow) {
        Object[] values = ArrayUtils.addAll(getOrderedColumnValues(newRow), getOrderedColumnValues(oldRow));
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
            } else {
                out.append("");
            }
        }
        return out.toString();
    }

    protected void buildNodeSelectSql() {
        StringBuilder b = new StringBuilder("select node_id from ");
        b.append(dbDialect.getTablePrefix());
        b.append("_node c, ");
        b.append(buildVirtualTableSql());
        b.append("where c.node_group_id='");
        b.append(trigger.getTargetGroupId());
        b.append("' and c.sync_enabled=1 ");
        b.append(replaceOldNewTriggerTokens(trigger.getNodeSelect()));
        this.nodeSelectSql = b.toString();
    }

    protected void buildDataSelectSql() {
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

    protected String replaceOldNewTriggerTokens(String targetString) {
        // This is a little hack to allow us to replace not only the old/new
        // alias's, but also the column prefix for
        // use in a virtual table we can match SQL expressions against.
        targetString = StringUtils.replace(targetString, "$(newTriggerValue).", "$(newTriggerValue)");
        targetString = StringUtils.replace(targetString, "$(oldTriggerValue).", "$(oldTriggerValue)");
        targetString = StringUtils.replace(targetString, "$(curTriggerValue).", "$(curTriggerValue)");
        return dbDialect.replaceTemplateVariables(triggerType, trigger, triggerHistory, targetString);
    }

    protected DataEventType getDataEventType(int type) {
        switch (type) {
        case org.h2.api.Trigger.INSERT:
            return DataEventType.INSERT;
        case org.h2.api.Trigger.UPDATE:
            return DataEventType.UPDATE;
        case org.h2.api.Trigger.DELETE:
            return DataEventType.DELETE;
        default:
            throw new IllegalStateException("Unexpected trigger type: " + type);
        }
    }

    protected H2Dialect getDbDialect() {
        return (H2Dialect) dbDialect;
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

    protected String getTransactionId(Connection c, Object[] oldRow, Object[] newRow) {
        if (transactionIdSql == null) {
            JdbcConnection con = (JdbcConnection) c;
            Session session = (Session) con.getSession();
            return String.format("%s-%s-%s", session.getId(), session.getFirstUncommittedLog(), session
                    .getFirstUncommittedPos());
        } else {
            return (String) getDbDialect().getJdbcTemplate().queryForObject(
                    fillVirtualTableSql(transactionIdSql, oldRow, newRow), String.class);
        }
    }
}
