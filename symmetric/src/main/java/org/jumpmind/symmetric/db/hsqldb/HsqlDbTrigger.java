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
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    boolean conditionalWhereExistsForUpdate;

    boolean conditionalWhereExistsForInsert;

    boolean conditionalWhereExistsForDelete;

    public void fire(int type, String triggerName, String tableName, Object[] oldRow, Object[] newRow) {
        try {
            init(type, triggerName, tableName);
            HsqlDbDialect dialect = getDbDialect();
            if (trigger.isSyncOnIncomingBatch() || dialect.isSyncEnabled() && isInsertDataEvent(oldRow, newRow)) {
                Data data = createData(oldRow, newRow);
                List<Node> nodes = findTargetNodes(oldRow, newRow);
                if (nodes != null) {
                    dataService.insertDataEvent(data, nodes);
                }
            }
        } catch (RuntimeException ex) {
            logger.error(ex, ex);
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    protected List<Node> findTargetNodes(Object[] oldRow, Object[] newRow) {
        Object[] values = null;
        switch (triggerType) {
        case INSERT:
            values = getOrderedColumnValues(newRow);
            break;
        case UPDATE:
            values = getOrderedColumnValues(ArrayUtils.add(newRow, oldRow));
            break;
        case DELETE:
            values = getOrderedColumnValues(oldRow);
            break;
        }
        return (List<Node>) getDbDialect().getJdbcTemplate().query(nodeSelectSql, values, new RowMapper() {
            public Object mapRow(ResultSet rs, int index) throws SQLException {
                Node node = new Node();
                node.setNodeId(rs.getString(1));
                return node;
            }
        });
    }

    private boolean isInsertDataEvent(Object[] oldRow, Object[] newRow) {
        Object[] values = null;
        switch (triggerType) {
        case INSERT:
            values = getOrderedColumnValues(newRow);
            break;
        case UPDATE:
            values = getOrderedColumnValues(ArrayUtils.add(newRow, oldRow));
            break;
        case DELETE:
            values = getOrderedColumnValues(oldRow);
            break;
        }
        return getDbDialect().getJdbcTemplate().queryForInt(dataSelectSql, values) > 0;
    }

    private void init(int type, String triggerName, String tableName) {
        if (this.triggerName == null) {
            this.triggerName = triggerName;
            this.initialize(getDataEventType(type), tableName);
            buildDataSelectSql();
            buildNodeSelectSql();
            logger.info("initializing " + triggerName + " " + this.hashCode() + " for " + triggerType);
        }
    }

    private String buildVirtualTableSql() {
        StringBuilder b = new StringBuilder("(select ");
        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.INSERT) {
            for (String column : includedColumns) {
                b.append("? as ");
                b.append("new_");
                b.append(column);
                b.append(",");
            }
        }

        if (triggerType == DataEventType.UPDATE || triggerType == DataEventType.DELETE) {
            for (String column : includedColumns) {
                b.append("? as ");
                b.append("old_");
                b.append(column);
                b.append(",");
            }
        }
        b.deleteCharAt(b.length() - 1);
        b.append(" from " + HsqlDbDialect.DUAL_TABLE + ") t ");
        return b.toString();
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
                conditionalWhereExistsForInsert = true;
                b.append("where ");
                b.append(trigger.getSyncOnInsertCondition());
            }
            break;
        case UPDATE:
            if (!StringUtils.isBlank(trigger.getSyncOnUpdateCondition())) {
                conditionalWhereExistsForUpdate = true;
                b.append("where ");
                b.append(trigger.getSyncOnUpdateCondition());
            }
            break;
        case DELETE:
            if (!StringUtils.isBlank(trigger.getSyncOnDeleteCondition())) {
                conditionalWhereExistsForDelete = true;
                b.append("where ");
                b.append(trigger.getSyncOnDeleteCondition());
            }
            break;
        }

        this.dataSelectSql = replaceOldNewTriggerTokens(b.toString());
    }

    private String replaceOldNewTriggerTokens(String b) {
        return StringUtils.replace(StringUtils.replace(b.toString(), "$(newTriggerValue).", "t.new_"),
                "$(oldTriggerValue).", "t.old_");
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

}
