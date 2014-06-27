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
package org.jumpmind.symmetric.statistic;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IStatisticService;

/**
 * @see IStatisticManager
 */
public class StatisticManager implements IStatisticManager {

    private static final String UNKNOWN = "Unknown";

    private Map<String, ChannelStats> channelStats = new HashMap<String, ChannelStats>();

    private List<JobStats> jobStats = new ArrayList<JobStats>();

    private HostStats hostStats;

    protected INodeService nodeService;

    protected IStatisticService statisticService;

    protected IParameterService parameterService;

    protected IConfigurationService configurationService;
    
    protected IClusterService clusterService;

    private static final int NUMBER_OF_PERMITS = 1000;

    Semaphore channelStatsLock = new Semaphore(NUMBER_OF_PERMITS, true);

    Semaphore hostStatsLock = new Semaphore(NUMBER_OF_PERMITS, true);

    Semaphore jobStatsLock = new Semaphore(NUMBER_OF_PERMITS, true);

    public StatisticManager(IParameterService parameterService, INodeService nodeService,
            IConfigurationService configurationService, IStatisticService statisticsService, IClusterService clusterService) {
        this.parameterService = parameterService;
        this.nodeService = nodeService;
        this.configurationService = configurationService;
        this.statisticService = statisticsService;
        this.clusterService = clusterService;
    }

    protected void init() {
        incrementRestart();
    }

    public void addJobStats(String jobName, long startTime, long endTime, long processedCount) {
        jobStatsLock.acquireUninterruptibly();
        try {
            JobStats stats = new JobStats(jobName, startTime, endTime, processedCount);
            jobStats.add(stats);
        } finally {
            jobStatsLock.release();
        }
    }

    public void incrementDataRouted(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataRouted(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void setDataUnRouted(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).setDataUnRouted(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataExtracted(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataExtracted(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataBytesExtracted(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesExtracted(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataExtractedErrors(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataExtractedErrors(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataEventInserted(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataEventInserted(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataSent(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataSent(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataBytesSent(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesSent(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataSentErrors(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataSentErrors(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataLoaded(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataLoaded(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataBytesLoaded(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesLoaded(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementDataLoadedErrors(String channelId, long count) {
        channelStatsLock.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataLoadedErrors(count);
        } finally {
            channelStatsLock.release();
        }
    }

    public void incrementRestart() {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementRestarted(1);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesPulled(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesPulled(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesPushed(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesPushed(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementTotalNodesPulledTime(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementTotalNodesPullTime(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementTotalNodesPushedTime(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementTotalNodesPushTime(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesRejected(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesRejected(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesRegistered(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesRegistered(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesLoaded(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesLoaded(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementNodesDisabled(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementNodesDisabled(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementPurgedBatchIncomingRows(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementPurgedBatchIncomingRows(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementPurgedBatchOutgoingRows(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementPurgedBatchOutgoingRows(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementPurgedDataRows(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementPurgedDataRows(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementPurgedDataEventRows(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementPurgedDataEventRows(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementTriggersRemovedCount(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementTriggersRemovedCount(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementTriggersRebuiltCount(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementTriggersRebuiltCount(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void incrementTriggersCreatedCount(long count) {
        hostStatsLock.acquireUninterruptibly();
        try {
            getHostStats().incrementTriggersCreatedCount(count);
        } finally {
            hostStatsLock.release();
        }
    }

    public void flush() {
        boolean recordStatistics = parameterService.is(ParameterConstants.STATISTIC_RECORD_ENABLE,
                false);
        if (channelStats != null) {
            channelStatsLock.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
                if (recordStatistics) {
                    Date endTime = new Date();
                    for (ChannelStats stats : channelStats.values()) {
                        if (stats.getNodeId().equals(UNKNOWN)) {
                            Node node = nodeService.getCachedIdentity();
                            if (node != null) {
                                stats.setNodeId(node.getNodeId());
                            }
                        }
                        stats.setEndTime(endTime);
                        statisticService.save(stats);
                    }
                }
                resetChannelStats(true);
            } finally {
                channelStatsLock.release(NUMBER_OF_PERMITS);
            }
        }

        if (hostStats != null) {
            hostStatsLock.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
                if (recordStatistics) {
                    if (hostStats.getNodeId().equals(UNKNOWN)) {
                        Node node = nodeService.getCachedIdentity();
                        if (node != null) {
                            hostStats.setNodeId(node.getNodeId());
                        }
                    }
                    hostStats.setEndTime(new Date());
                    statisticService.save(hostStats);
                }
                hostStats = null;
            } finally {
                hostStatsLock.release(NUMBER_OF_PERMITS);
            }
        }

        if (jobStats != null) {
            List<JobStats> toFlush = null;
            jobStatsLock.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
                toFlush = jobStats;
                jobStats = new ArrayList<JobStats>();
            } finally {
                jobStatsLock.release(NUMBER_OF_PERMITS);
            }

            if (toFlush != null && recordStatistics) {
                Node node = nodeService.getCachedIdentity();
                if (node != null) {
                    String nodeId = node.getNodeId();
                    String serverId = clusterService.getServerId();
                    for (JobStats stats : toFlush) {
                        stats.setNodeId(nodeId);
                        stats.setHostName(serverId);
                        statisticService.save(stats);
                    }
                }
            }
        }
    }

    public Map<String, ChannelStats> getWorkingChannelStats() {
        if (channelStats != null) {
            return new HashMap<String, ChannelStats>(channelStats);
        } else {
            return new HashMap<String, ChannelStats>();
        }
    }

    public HostStats getWorkingHostStats() {
        if (this.hostStats != null) {
            return new HostStats(this.hostStats);
        } else {
            return new HostStats();
        }
    }

    protected void resetChannelStats(boolean force) {
        if (force) {
            channelStats = null;
        }

        if (channelStats == null) {
            List<NodeChannel> channels = configurationService.getNodeChannels(false);
            channelStats = new HashMap<String, ChannelStats>(channels.size());
            for (NodeChannel nodeChannel : channels) {
                getChannelStats(nodeChannel.getChannelId());
            }
        }
    }

    protected ChannelStats getChannelStats(String channelId) {
        resetChannelStats(false);
        ChannelStats stats = channelStats.get(channelId);
        if (stats == null) {
            Node node = nodeService.getCachedIdentity();
            if (node != null) {
                stats = new ChannelStats(node.getNodeId(), clusterService.getServerId(), new Date(),
                        null, channelId);
                channelStats.put(channelId, stats);
            } else {
                stats = new ChannelStats(UNKNOWN, clusterService.getServerId(), new Date(), null,
                        channelId);
            }

        }
        return stats;
    }

    protected HostStats getHostStats() {
        if (hostStats == null) {
            Node node = nodeService.getCachedIdentity();
            if (node != null) {
                hostStats = new HostStats(node.getNodeId(), clusterService.getServerId(), new Date(),
                        null);
            } else {
                hostStats = new HostStats(UNKNOWN, clusterService.getServerId(), new Date(), null);
            }

        }
        return hostStats;
    }

}