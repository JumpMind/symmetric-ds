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

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;

public class HsqlDbTrigger implements org.hsqldb.Trigger {

    static final Log logger = LogFactory.getLog(HsqlDbTrigger.class);

    public void fire(int type, String triggerName, String tableName,
            Object[] oldRow, Object[] newRow) {
        try {
            logger.info("trigger " + triggerName + " fired for " + tableName);
            SymmetricEngine engine = getEngine(triggerName);
            IConfigurationService configService = getConfigurationService(engine);
            IDataService dataService = getDataService(engine);
            INodeService nodeService = getNodeService(engine);
            TriggerHistory history = configService
                    .getHistoryRecordFor(getTriggerHistoryId(triggerName));
            Trigger trigger = configService.getTriggerById(history
                    .getTriggerId());
            HsqlDbDialect dialect = getDbDialect(engine);
            if (trigger.isSyncOnIncomingBatch() || dialect.isSyncEnabled()) {
                DataEventType eventType = getDataEventType(type);
                Data data = new Data(trigger.getChannelId(), tableName,
                        eventType, formatRowData(eventType, oldRow, newRow),
                        formatPkRowData(eventType, oldRow, newRow), history);

                // select nodes from sym_node

                DataEventAction action = configService
                        .getDataEventActionsByGroupId(trigger
                                .getSourceGroupId(), trigger.getTargetGroupId());
                List<Node> nodes = null;

                if (action != null) {
                    switch (action) {
                    case PUSH:
                        //nodes = nodeService.findNodesToPushTo();
                        break;
                    case WAIT_FOR_POLL:
                        //nodes = nodeService.findNodesToPull();
                    }
                }
                if (nodes != null) {
                    dataService.insertDataEvent(data, nodes);
                }

            }
        } catch (RuntimeException ex) {
            logger.error(ex, ex);
            throw ex;
        }
    }

    private String formatRowData(DataEventType type, Object[] oldRow,
            Object[] newRow) {
        return null;
    }

    private String formatPkRowData(DataEventType type, Object[] oldRow,
            Object[] newRow) {
        return null;
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

    private int getTriggerHistoryId(String triggerName) {
        return Integer.parseInt(triggerName.substring(triggerName
                .lastIndexOf("_") + 1));
    }

    private HsqlDbDialect getDbDialect(SymmetricEngine engine) {
        return (HsqlDbDialect) engine.getApplicationContext().getBean(
                Constants.DB_DIALECT);
    }

    private IConfigurationService getConfigurationService(SymmetricEngine engine) {
        return (IConfigurationService) engine.getApplicationContext().getBean(
                Constants.CONFIG_SERVICE);
    }

    private INodeService getNodeService(SymmetricEngine engine) {
        return (INodeService) engine.getApplicationContext().getBean(
                Constants.NODE_SERVICE);
    }

    private IDataService getDataService(SymmetricEngine engine) {
        return (IDataService) engine.getApplicationContext().getBean(
                Constants.DATA_SERVICE);
    }

    private SymmetricEngine getEngine(String triggerName) {
        String minusTriggerId = triggerName.substring(0, triggerName
                .lastIndexOf("_"));
        String engineName = minusTriggerId.substring(minusTriggerId
                .lastIndexOf("_") + 1);
        return SymmetricEngine.findEngineByName(engineName.toLowerCase());
    }

}
