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
package org.jumpmind.symmetric.statistic;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.INotificationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * @see IStatisticManager
 */
public class StatisticManager implements IStatisticManager {
    
    private static final String UNKNOWN = "Unknown";

    static final ILog log = LogFactory.getLog(StatisticManager.class);

    private Map<String, ChannelStats> channelStats = new HashMap<String, ChannelStats>();
    
    private HostStats hostStats;

    protected INodeService nodeService;

    protected IStatisticService statisticService;

    protected INotificationService notificationService;

    protected IParameterService parameterService;

    protected IConfigurationService configurationService;

    private static final int NUMBER_OF_PERMITS = 1000;

    Semaphore channelStatsLock = new Semaphore(NUMBER_OF_PERMITS, true);
    
    Semaphore hostStatsLock = new Semaphore(NUMBER_OF_PERMITS, true);

    public StatisticManager() {
    }
    
    protected void init() {
        incrementRestart();
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
        getChannelStats(channelId).setDataUnRouted(count);
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

    public void flush() {
        if (channelStats != null) {
            channelStatsLock.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
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
                    resetChannelStats(true);
                }
            } finally {
                channelStatsLock.release(NUMBER_OF_PERMITS);
            }
        }
        
        if (hostStats != null) {
            hostStatsLock.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
                if (hostStats.getNodeId().equals(UNKNOWN)) {
                    Node node = nodeService.getCachedIdentity();
                    if (node != null) {
                        hostStats.setNodeId(node.getNodeId());
                    }
                }
                hostStats.setEndTime(new Date());
                statisticService.save(hostStats);
                hostStats = null;
            } finally {
                hostStatsLock.release(NUMBER_OF_PERMITS);
            }
        }        
    }
    
    public Map<String, ChannelStats> getWorkingChannelStats() {
        return new HashMap<String, ChannelStats>(channelStats);
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
                stats = new ChannelStats(node.getNodeId(), AppUtils.getServerId(), new Date(), null, channelId);
                channelStats.put(channelId, stats);
            } else {
                log.warn("StatisticNodeNotAvailableWarning");
                stats = new ChannelStats(UNKNOWN, AppUtils.getServerId(), new Date(), null, channelId);
            }

        }
        return stats;
    }
    
    protected HostStats getHostStats() {
        resetChannelStats(false);
        if (hostStats == null) {
            Node node = nodeService.getCachedIdentity();
            if (node != null) {
                hostStats = new HostStats(node.getNodeId(), AppUtils.getServerId(), new Date(), null);
            } else {
                log.warn("StatisticNodeNotAvailableWarning");
                hostStats = new HostStats(UNKNOWN, AppUtils.getServerId(), new Date(), null);
            }

        }
        return hostStats;
    }    

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setStatisticService(IStatisticService statisticService) {
        this.statisticService = statisticService;
    }

    public void setNotificationService(INotificationService notificationService) {
        this.notificationService = notificationService;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}