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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerRouter;

public class ChannelRouterContext extends SimpleRouterContext {

    public static final String STAT_INSERT_DATA_EVENTS_MS = "data.events.insert.time.ms";
    public static final String STAT_DATA_ROUTER_MS = "data.router.time.ms";
    public static final String STAT_QUERY_TIME_MS = "data.read.query.time.ms";
    public static final String STAT_READ_DATA_MS = "data.read.total.time.ms";
    public static final String STAT_REREAD_DATA_MS = "data.reread.time.ms";
    public static final String STAT_ENQUEUE_DATA_MS = "data.enqueue.time.ms";
    public static final String STAT_ENQUEUE_EOD_MS = "data.enqueue.eod.time.ms";
    public static final String STAT_DATA_EVENTS_INSERTED = "data.events.insert.count";
    public static final String STAT_DATA_ROUTED_COUNT = "data.routed.count";
    public static final String STAT_ROUTE_TOTAL_TIME = "total.time.ms";

    private Map<String, OutgoingBatch> batchesByNodes = new HashMap<String, OutgoingBatch>();
    private Map<TriggerRouter, Set<Node>> availableNodes = new HashMap<TriggerRouter, Set<Node>>();
    private Set<IDataRouter> usedDataRouters = new HashSet<IDataRouter>();
    private ISqlTransaction sqlTransaction;
    private boolean needsCommitted = false;
    private long createdTimeInMs = System.currentTimeMillis();
    private long lastDataIdProcessed;
    private Map<String, Long> transactionIdDataIds = new HashMap<String, Long>();
    private List<DataEvent> dataEventsToSend = new ArrayList<DataEvent>();
    private boolean produceCommonBatches = false;

    public ChannelRouterContext(String nodeId, NodeChannel channel, ISqlTransaction transaction)
            throws SQLException {
        super(nodeId, channel);
        this.sqlTransaction = transaction;
        this.sqlTransaction.setInBatchMode(true);
    }

    public List<DataEvent> getDataEventList() {
        return dataEventsToSend;
    }

    public void clearDataEventsList() {
        dataEventsToSend.clear();
    }

    public void addDataEvent(long dataId, long batchId, String routerId) {
        dataEventsToSend.add(new DataEvent(dataId, batchId, routerId));
    }

    public Map<String, OutgoingBatch> getBatchesByNodes() {
        return batchesByNodes;
    }

    public Map<TriggerRouter, Set<Node>> getAvailableNodes() {
        return availableNodes;
    }

    public void commit() {
        try {
            sqlTransaction.commit();
        } finally {
            clearState();
        }
    }

    protected void clearState() {
        this.usedDataRouters.clear();
        this.encountedTransactionBoundary = false;
        this.batchesByNodes.clear();
        this.availableNodes.clear();
        this.dataEventsToSend.clear();
    }

    public void rollback() {
        try {
            sqlTransaction.rollback();
        } catch (SqlException e) {
            log.warn(e.getMessage(),e);
        } finally {
            clearState();
        }
    }

    public void cleanup() {
        try {
            this.sqlTransaction.commit();
        } catch (Exception ex) {
            log.warn(ex.getMessage(),ex);
        } finally {
            this.sqlTransaction.close();
        }
    }

    public void setNeedsCommitted(boolean b) {
        this.needsCommitted = b;
    }

    public boolean isNeedsCommitted() {
        return needsCommitted;
    }

    public Set<IDataRouter> getUsedDataRouters() {
        return usedDataRouters;
    }

    public void addUsedDataRouter(IDataRouter dataRouter) {
        this.usedDataRouters.add(dataRouter);
    }

    public void resetForNextData() {
        this.needsCommitted = false;
    }

    public long getCreatedTimeInMs() {
        return createdTimeInMs;
    }

    public void setLastDataIdForTransactionId(Data data) {
        if (data.getTransactionId() != null) {
            this.transactionIdDataIds.put(data.getTransactionId(), data.getDataId());
        }
    }

    public void recordTransactionBoundaryEncountered(Data data) {
        Long dataId = transactionIdDataIds.get(data.getTransactionId());
        setEncountedTransactionBoundary(dataId == null ? true : dataId == data.getDataId());
    }

    public void setLastDataIdProcessed(long lastDataIdProcessed) {
        this.lastDataIdProcessed = lastDataIdProcessed;
    }

    public long getLastDataIdProcessed() {
        return lastDataIdProcessed;
    }
    
    public ISqlTransaction getSqlTransaction() {
        return sqlTransaction;
    }
    
    public void setProduceCommonBatches(boolean defaultRoutersOnly) {
        this.produceCommonBatches = defaultRoutersOnly;
    }
    
    public boolean isProduceCommonBatches() {
        return produceCommonBatches;
    }
    
}
