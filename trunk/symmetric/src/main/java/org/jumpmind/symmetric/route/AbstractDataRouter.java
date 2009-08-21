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
package org.jumpmind.symmetric.route;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;

public abstract class AbstractDataRouter implements IDataRouter {

    private static final String OLD_ = "OLD_";

    protected final ILog log = LogFactory.getLog(getClass());

    private boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    protected Map<String, String> getNewDataAsString(DataMetaData dataMetaData) {
        String[] rowData = dataMetaData.getData().getParsedRowData();
        String[] columns = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            String name = columns[i].toUpperCase();
            map.put(name, rowData[i]);
        }
        return map;
    }

    protected Map<String, Object> getDataObjectMap(DataMetaData dataMetaData, IDbDialect dbDialect) {
        Map<String, Object> data = null;
        DataEventType dml = dataMetaData.getData().getEventType();
        switch (dml) {
        case UPDATE:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewData(null, dataMetaData, dbDialect));
            data.putAll(getOldData(OLD_, dataMetaData, dbDialect));
            break;
        case INSERT:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewData(null, dataMetaData, dbDialect));
            data.putAll(getNullData(OLD_, dataMetaData));
            break;
        case DELETE:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getOldData(null, dataMetaData, dbDialect));
            data.putAll(getOldData(OLD_, dataMetaData, dbDialect));
            break;
        default:
            break;
        }
        return data;
    }

    protected Map<String, Object> getNewData(String prefix, DataMetaData dataMetaData, IDbDialect dbDialect) {
        return getData(prefix, dataMetaData, dbDialect, dataMetaData.getData().getParsedRowData());
    }

    protected Map<String, Object> getOldData(String prefix, DataMetaData dataMetaData, IDbDialect dbDialect) {
        return getData(prefix, dataMetaData, dbDialect, dataMetaData.getData().getParsedOldData());
    }

    protected Map<String, Object> getNullData(String prefix, DataMetaData dataMetaData) {
        String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Map<String, Object> data = new HashMap<String, Object>(columnNames.length);
        for (String columnName : columnNames) {
            data.put(prefix != null ? prefix + columnName : columnName, null);
        }
        return data;
    }

    protected Map<String, Object> getData(String prefix, DataMetaData dataMetaData, IDbDialect dbDialect,
            String[] rowData) {
        if (rowData != null) {
            Map<String, Object> data = new HashMap<String, Object>(rowData.length);
            String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
            Object[] objects = dbDialect.getObjectValues(dbDialect.getBinaryEncoding(), dataMetaData.getTable(),
                    columnNames, rowData);
            for (int i = 0; i < columnNames.length; i++) {
                data.put(prefix != null ? (prefix + columnNames[i]).toUpperCase() : columnNames[i].toUpperCase(), objects[i]);
            }
            return data;
        } else {
            return Collections.emptyMap();
        }
    }

    protected Set<String> toNodeIds(Set<Node> nodes) {
        Set<String> nodeIds = new HashSet<String>(nodes.size());
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }

    /**
     * Override if needed.
     */
    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
        log.debug("BatchCompleting", batch.getBatchId());
    }
}
