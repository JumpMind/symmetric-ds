/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */

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

/**
 * A common superclass for data routers
 */
public abstract class AbstractDataRouter implements IDataRouter {

    private static final String OLD_ = "OLD_";

    protected ILog log = LogFactory.getLog(getClass());

    private boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }
    
    public void contextCommitted(IRouterContext context) {
    }

    protected Map<String, String> getDataMap(DataMetaData dataMetaData) {
        Map<String, String> data = null;
        DataEventType dml = dataMetaData.getData().getEventType();
        switch (dml) {
        case UPDATE:
            data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewDataAsString(null, dataMetaData));
            data.putAll(getOldDataAsString(OLD_, dataMetaData));
            break;
        case INSERT:
            data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewDataAsString(null, dataMetaData));
            Map<String, String> map = getNullData(OLD_, dataMetaData);
            data.putAll(map);
            break;
        case DELETE:
            data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getOldDataAsString(null, dataMetaData));
            data.putAll(getOldDataAsString(OLD_, dataMetaData));
            break;
        default:
            data = new HashMap<String, String>(1);
            break;
        }
        
        if (data.size() == 0) {
            data.putAll(getPkDataAsString(dataMetaData));
        }
        data.put("EXTERNAL_DATA", dataMetaData.getData().getExternalData());
        return data;
    }

    protected Map<String, String> getNewDataAsString(String prefix, DataMetaData dataMetaData) {
        String[] rowData = dataMetaData.getData().toParsedRowData();
        return getDataAsString(prefix, dataMetaData, rowData);
    }

    protected Map<String, String> getOldDataAsString(String prefix, DataMetaData dataMetaData) {
        String[] rowData = dataMetaData.getData().toParsedOldData();
        return getDataAsString(prefix, dataMetaData, rowData);
    }

    protected Map<String, String> getDataAsString(String prefix, DataMetaData dataMetaData,
            String[] rowData) {
        String[] columns = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        if (rowData != null) {
            for (int i = 0; i < columns.length; i++) {
                String columnName = columns[i].toUpperCase();
                map.put(prefix != null ? prefix + columnName : columnName, rowData[i]);
            }
        }
        return map;
    }
    
    protected Map<String, String> getPkDataAsString(DataMetaData dataMetaData) {
        String[] columns = dataMetaData.getTriggerHistory().getParsedPkColumnNames();
        String[] rowData = dataMetaData.getData().toParsedPkData();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        if (rowData != null) {
            for (int i = 0; i < columns.length; i++) {
                String columnName = columns[i].toUpperCase();
                map.put(columnName, rowData[i]);
            }
        }
        return map;
    }

    protected Map<String, Object> getDataObjectMap(DataMetaData dataMetaData, IDbDialect dbDialect) {
        Map<String, Object> data = null;
        DataEventType dml = dataMetaData.getData().getEventType();
        switch (dml) {
        case UPDATE:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewDataAsObject(null, dataMetaData, dbDialect));
            data.putAll(getOldDataAsObject(OLD_, dataMetaData, dbDialect));
            break;
        case INSERT:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getNewDataAsObject(null, dataMetaData, dbDialect));
            Map<String, Object> map = getNullData(OLD_, dataMetaData);
            data.putAll(map);
            break;
        case DELETE:
            data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
            data.putAll(getOldDataAsObject(null, dataMetaData, dbDialect));
            data.putAll(getOldDataAsObject(OLD_, dataMetaData, dbDialect));
            if (data.size() == 0) {
                data.putAll(getPkDataAsObject(dataMetaData, dbDialect));
            }            
            break;
        default:
            break;
        }
        return data;
    }

    protected Map<String, Object> getNewDataAsObject(String prefix, DataMetaData dataMetaData,
            IDbDialect dbDialect) {
        return getDataAsObject(prefix, dataMetaData, dbDialect, dataMetaData.getData()
                .toParsedRowData());
    }

    protected Map<String, Object> getOldDataAsObject(String prefix, DataMetaData dataMetaData,
            IDbDialect dbDialect) {
        return getDataAsObject(prefix, dataMetaData, dbDialect, dataMetaData.getData()
                .toParsedOldData());
    }

    protected <T> Map<String, T> getNullData(String prefix, DataMetaData dataMetaData) {
        String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Map<String, T> data = new HashMap<String, T>(columnNames.length);
        for (String columnName : columnNames) {
            data.put(prefix != null ? prefix + columnName : columnName, null);
        }
        return data;
    }

    protected Map<String, Object> getDataAsObject(String prefix, DataMetaData dataMetaData,
            IDbDialect dbDialect, String[] rowData) {
        if (rowData != null) {
            Map<String, Object> data = new HashMap<String, Object>(rowData.length);
            String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
            Object[] objects = dbDialect.getObjectValues(dbDialect.getBinaryEncoding(),
                    dataMetaData.getTable(), columnNames, rowData);
            for (int i = 0; i < columnNames.length; i++) {
                String upperCase = columnNames[i].toUpperCase();
                data.put(prefix != null ? (prefix + upperCase) : upperCase, objects[i]);
            }
            return data;
        } else {
            return Collections.emptyMap();
        }
    }
    
    protected Map<String, Object> getPkDataAsObject(DataMetaData dataMetaData,
            IDbDialect dbDialect) {
        String[] rowData = dataMetaData.getData().toParsedPkData();
        if (rowData != null) {
            Map<String, Object> data = new HashMap<String, Object>(rowData.length);
            String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
            Object[] objects = dbDialect.getObjectValues(dbDialect.getBinaryEncoding(),
                    dataMetaData.getTable(), columnNames, rowData);
            for (int i = 0; i < columnNames.length; i++) {
                data.put(columnNames[i]
                        .toUpperCase(), objects[i]);
            }
            return data;
        } else {
            return Collections.emptyMap();
        }
    }

    protected Set<String> addNodeId(String nodeId, Set<String> nodeIds, Set<Node> nodes) {
        nodeIds = nodeIds == null ? new HashSet<String>(1) : nodeIds;
        for (Node node : nodes) {
            if (node.getNodeId().equals(nodeId)) {
                nodeIds.add(nodeId);
                break;
            }
        }
        return nodeIds;
    }

    protected Set<String> toNodeIds(Set<Node> nodes, Set<String> nodeIds) {
        nodeIds = nodeIds == null ? new HashSet<String>(nodes.size()) : nodeIds;
        for (Node node : nodes) {
            nodeIds.add(node.getNodeId());
        }
        return nodeIds;
    }

    protected Set<String> toExternalIds(Set<Node> nodes) {
        Set<String> externalIds = new HashSet<String>();
        for (Node node : nodes) {
            externalIds.add(node.getExternalId());
        }
        return externalIds;
    }

    /**
     * Override if needed.
     */
    public void completeBatch(IRouterContext context, OutgoingBatch batch) {
        log.debug("BatchCompleting", batch.getBatchId());
    }
}