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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEvent;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * 
 */
public class ChannelRouterContext extends SimpleRouterContext implements IRouterContext {

    static final ILog log = LogFactory.getLog(ChannelRouterContext.class);
    
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
    private Connection connection;
    private boolean needsCommitted = false;
    private long createdTimeInMs = System.currentTimeMillis();
    private long lastDataIdProcessed;
    private Map<String, Long> transactionIdDataIds = new HashMap<String, Long>();
    private List<DataEvent> dataEventsToSend = new ArrayList<DataEvent>();
    private boolean oldAutoCommitSetting = false;

    public ChannelRouterContext(String nodeId, NodeChannel channel, DataSource dataSource)
            throws SQLException {
        this.connection = dataSource.getConnection();
        this.oldAutoCommitSetting = this.connection.getAutoCommit();
        this.connection.setAutoCommit(false);
        this.init(new JdbcTemplate(new SingleConnectionDataSource(connection, true)), channel,
                nodeId);
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

    public void commit() throws SQLException {
        try {
           connection.commit();
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
            connection.rollback();
        } catch (SQLException e) {
            LogFactory.getLog(getClass()).warn(e);
        } finally {
            clearState();
        }
    }

    public void cleanup() {
        try {
            this.connection.commit();
            this.connection.setAutoCommit(oldAutoCommitSetting);
        } catch (Exception ex) {
            log.warn(ex);
        }
        JdbcUtils.closeConnection(this.connection);
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
}