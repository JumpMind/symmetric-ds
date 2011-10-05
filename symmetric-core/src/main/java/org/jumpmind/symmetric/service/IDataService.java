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
package org.jumpmind.symmetric.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * This service provides an API to access and update {@link Data}.
 */
public interface IDataService {
    
    public String reloadNode(String nodeId);

    
    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName);

    public String reloadTable(String nodeId, String catalogName, String schemaName, String tableName, String overrideInitialLoadSelect);

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName, String sql, boolean isLoad);

    public void insertReloadEvents(Node targetNode);

    public void insertReloadEvent(Node targetNode, TriggerRouter trigger);
    
    public void sendScript(String nodeId, String script, boolean isLoad);

    /**
     * Update {@link Node} information for this node and call {@link IHeartbeatListener}s.
     */
    public void heartbeat(boolean force);

    public void insertHeartbeatEvent(Node node, boolean isReload);

    public long insertData(Data data);

    public void insertDataEvent(long dataId, long batchId, String routerId);

    public void insertDataEvent(JdbcTemplate template, long dataId, long batchId, String routerId);

    public void insertDataEvents(JdbcTemplate template, List<DataEvent> events);
    
    public void insertDataEventAndOutgoingBatch(long dataId, String channelId, String nodeId, DataEventType eventType, String routerId, boolean isLoad);

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String channelId, List<Node> nodes, String routerId, boolean isLoad);

    public void insertDataAndDataEventAndOutgoingBatch(Data data, String nodeId, String routerId, boolean isLoad);

    public void insertPurgeEvent(Node targetNode, TriggerRouter triggerRouter, boolean isLoad);

    public void insertSqlEvent(Node targetNode, Trigger trigger, String sql, boolean isLoad);

    public void insertSqlEvent(Node targetNode, String sql, boolean isLoad);

    public void insertCreateEvent(Node targetNode, TriggerRouter triggerRouter, String xml, boolean isLoad);
    
    /**
     * Count the number of data ids in a range
     */
    public int countDataInRange(long firstDataId, long secondDataId);
    
    public void checkForAndUpdateMissingChannelIds(long firstDataId, long lastDataId);

    public void saveDataRef(DataRef dataRef);

    public DataRef getDataRef();
    
    public List<DataGap> findDataGapsByStatus(DataGap.Status status);
    
    public List<DataGap> findDataGaps();

    public Date findCreateTimeOfEvent(long dataId);
    
    public Date findCreateTimeOfData(long dataId);

    public Data createData(String catalogName, String schemaName, String tableName);

    public Data createData(String catalogName, String schemaName, String tableName, String whereClause);

    public Map<String, String> getRowDataAsMap(Data data);

    public void setRowDataFromMap(Data data, Map<String, String> map);

    public void addReloadListener(IReloadListener listener);
    
    public void addHeartbeatListener(IHeartbeatListener listener);

    public void setReloadListeners(List<IReloadListener> listeners);

    public boolean removeReloadListener(IReloadListener listener);

    public Data readData(ResultSet results) throws SQLException;
    
    public List<Number> listDataIds(long batchId, boolean descending);
    
    public List<Data> listData(long batchId, long startDataId, String channelId, boolean descending, int maxRowsToRetrieve);
    
    public void handleDataSelect(final long batchId, final long startDataId, final String channelId, final boolean descending,  
            final IModelRetrievalHandler<Data, String> handler);
    
    public void insertDataGap(DataGap gap);
    
    public void updateDataGap(DataGap gap, DataGap.Status status);
    
    public long findMaxDataId();
    
    public IParameterService getParameterService();
    
    public IDbDialect getDbDialect();
    
    public JdbcTemplate getJdbcTemplate();

}