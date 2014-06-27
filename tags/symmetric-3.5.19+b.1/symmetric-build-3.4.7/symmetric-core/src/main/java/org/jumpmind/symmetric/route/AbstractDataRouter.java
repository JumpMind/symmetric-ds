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
 * under the License. 
 */

package org.jumpmind.symmetric.route;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.DataMetaData;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A common superclass for data routers
 */
public abstract class AbstractDataRouter implements IDataRouter {

    private static final String OLD_ = "OLD_";

    protected Logger log = LoggerFactory.getLogger(getClass());

    public void contextCommitted(SimpleRouterContext context) {
    }

    protected Map<String, String> getDataMap(DataMetaData dataMetaData, ISymmetricDialect symmetricDialect) {
        Map<String, String> data = null;
        DataEventType dml = dataMetaData.getData().getDataEventType();
        switch (dml) {
            case UPDATE:
                data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getNewDataAsString(null, dataMetaData, symmetricDialect));
                data.putAll(getOldDataAsString(OLD_, dataMetaData, symmetricDialect));
                break;
            case INSERT:
                data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getNewDataAsString(null, dataMetaData, symmetricDialect));
                Map<String, String> map = getNullData(OLD_, dataMetaData);
                data.putAll(map);
                break;
            case DELETE:
                data = new HashMap<String, String>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getOldDataAsString(null, dataMetaData, symmetricDialect));
                data.putAll(getOldDataAsString(OLD_, dataMetaData, symmetricDialect));
                break;
            default:
                data = new HashMap<String, String>(1);
                break;
        }

        if (data.size() == 0) {
            data.putAll(getPkDataAsString(dataMetaData, symmetricDialect));
        }
        data.put("EXTERNAL_DATA", dataMetaData.getData().getExternalData());
        return data;
    }

    protected Map<String, String> getNewDataAsString(String prefix, DataMetaData dataMetaData, ISymmetricDialect symmetricDialect) {
        String[] rowData = dataMetaData.getData().toParsedRowData();
        return getDataAsString(prefix, dataMetaData, symmetricDialect, rowData);
    }

    protected Map<String, String> getOldDataAsString(String prefix, DataMetaData dataMetaData, ISymmetricDialect symmetricDialect) {
        String[] rowData = dataMetaData.getData().toParsedOldData();
        return getDataAsString(prefix, dataMetaData, symmetricDialect, rowData);
    }

    protected Map<String, String> getDataAsString(String prefix, DataMetaData dataMetaData, ISymmetricDialect symmetricDialect,
            String[] rowData) {
        String[] columns = dataMetaData.getTriggerHistory().getParsedColumnNames();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        if (rowData != null) {
            testColumnNamesMatchValues(dataMetaData, symmetricDialect, columns, rowData);
            for (int i = 0; i < columns.length; i++) {
                String columnName = columns[i].toUpperCase();
                map.put(prefix != null ? prefix + columnName : columnName, rowData[i]);
            }
        }
        return map;
    }

    protected Map<String, String> getPkDataAsString(DataMetaData dataMetaData, ISymmetricDialect symmetricDialect) {
        String[] columns = dataMetaData.getTriggerHistory().getParsedPkColumnNames();
        String[] rowData = dataMetaData.getData().toParsedPkData();
        Map<String, String> map = new HashMap<String, String>(columns.length);
        if (rowData != null) {
            testColumnNamesMatchValues(dataMetaData, symmetricDialect, columns, rowData);
            for (int i = 0; i < columns.length; i++) {
                String columnName = columns[i].toUpperCase();
                map.put(columnName, rowData[i]);
            }
        }
        return map;
    }

    protected Map<String, Object> getDataObjectMap(DataMetaData dataMetaData,
            ISymmetricDialect symmetricDialect, boolean upperCase) {
        Map<String, Object> data = null;
        DataEventType dml = dataMetaData.getData().getDataEventType();
        switch (dml) {
            case UPDATE:
                data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getNewDataAsObject(null, dataMetaData, symmetricDialect, upperCase));
                data.putAll(getOldDataAsObject(OLD_, dataMetaData, symmetricDialect, upperCase));
                break;
            case INSERT:
                data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getNewDataAsObject(null, dataMetaData, symmetricDialect, upperCase));
                Map<String, Object> map = getNullData(OLD_, dataMetaData);
                data.putAll(map);
                break;
            case DELETE:
                data = new HashMap<String, Object>(dataMetaData.getTable().getColumnCount() * 2);
                data.putAll(getOldDataAsObject(null, dataMetaData, symmetricDialect, upperCase));
                data.putAll(getOldDataAsObject(OLD_, dataMetaData, symmetricDialect, upperCase));
                if (data.size() == 0) {
                    data.putAll(getPkDataAsObject(dataMetaData, symmetricDialect));
                }
                break;
            default:
                break;
        }
        
        if (data != null && data.size() == 0) {
            data.putAll(getPkDataAsString(dataMetaData, symmetricDialect));
        }
        
        if (StringUtils.isNotBlank(dataMetaData.getData().getExternalData())) {
        	if (data == null) {
        		data = new HashMap<String, Object>(1);
        	}
            data.put("EXTERNAL_DATA", dataMetaData.getData().getExternalData());        	
        }
        return data;
    }

    protected Map<String, Object> getNewDataAsObject(String prefix, DataMetaData dataMetaData,
            ISymmetricDialect symmetricDialect, boolean upperCase) {
        return getDataAsObject(prefix, dataMetaData, symmetricDialect, dataMetaData.getData()
                .toParsedRowData(), upperCase);
    }

    protected Map<String, Object> getOldDataAsObject(String prefix, DataMetaData dataMetaData,
            ISymmetricDialect symmetricDialect, boolean upperCase) {
        return getDataAsObject(prefix, dataMetaData, symmetricDialect, dataMetaData.getData()
                .toParsedOldData(), upperCase);
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
            ISymmetricDialect symmetricDialect, String[] rowData, boolean upperCase) {
        if (rowData != null) {
            Map<String, Object> data = new HashMap<String, Object>(rowData.length);
            String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
            Object[] objects = symmetricDialect.getPlatform().getObjectValues(
                    symmetricDialect.getBinaryEncoding(), dataMetaData.getTable(), columnNames,
                    rowData);
            testColumnNamesMatchValues(dataMetaData, symmetricDialect, columnNames, objects);
            for (int i = 0; i < columnNames.length; i++) {
                String colName = upperCase ? columnNames[i].toUpperCase() : columnNames[i];
                data.put(prefix != null ? (prefix + colName) : colName, objects[i]);
            }
            return data;
        } else {
            return Collections.emptyMap();
        }
    }

    protected void testColumnNamesMatchValues(DataMetaData dataMetaData, ISymmetricDialect symmetricDialect, String[] columnNames, Object[] values) {
        if (columnNames.length != values.length) {
            String additionalErrorMessage = "";
            String triggerHistTableName = dataMetaData.getTriggerHistory().getFullyQualifiedSourceTableName();
            String triggerTableName = dataMetaData.getTriggerRouter().getTrigger().getFullyQualifiedSourceTableName();
            if (!triggerHistTableName.equalsIgnoreCase(triggerTableName)) {
                additionalErrorMessage += String.format("\nThe trigger hist table name (%s) does not match the trigger table name (%s).  Did the trigger hist table get reset and while the data table did not?", triggerHistTableName, triggerHistTableName);                
            }
            if (symmetricDialect != null && 
                    symmetricDialect.getPlatform().getName().equals(DatabaseNamesConstants.ORACLE)) {
                boolean isContainsBigLobs = dataMetaData.getNodeChannel().isContainsBigLob();
                additionalErrorMessage += String.format("\nOne possible cause of this issue is when channel.contains_big_lobs=0 and the captured row_data size exceeds 4k, captured data will be truncated at 4k. channel.contains_big_lobs is currently set to %s.", isContainsBigLobs ? "1" : "0");
            }
            String message = String.format(
                    "The number of recorded column names (%d) did not match the number of captured data values (%d).  The data_id %d failed for an %s on %s. %s\ncolumn_names:\n%s\nvalues:\n%s",
                    columnNames.length, values.length,
                            dataMetaData.getData().getDataId(),
                            dataMetaData.getData().getDataEventType().name(),
                            dataMetaData.getData().getTableName(),
                            additionalErrorMessage,
                            ArrayUtils.toString(columnNames), ArrayUtils.toString(values));
            throw new SymmetricException(message);
        }
    }

    protected Map<String, Object> getPkDataAsObject(DataMetaData dataMetaData,
            ISymmetricDialect symmetricDialect) {
        String[] rowData = dataMetaData.getData().toParsedPkData();
        if (rowData != null) {
            Map<String, Object> data = new HashMap<String, Object>(rowData.length);
            String[] columnNames = dataMetaData.getTriggerHistory().getParsedColumnNames();
            Object[] objects = symmetricDialect.getPlatform().getObjectValues(
                    symmetricDialect.getBinaryEncoding(), dataMetaData.getTable(), columnNames,
                    rowData);
            testColumnNamesMatchValues(dataMetaData, symmetricDialect, columnNames, objects);
            for (int i = 0; i < columnNames.length; i++) {
                data.put(columnNames[i].toUpperCase(), objects[i]);
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
    public void completeBatch(SimpleRouterContext context, OutgoingBatch batch) {
        log.debug("Completing batch {}", batch.getBatchId());
    }
}
