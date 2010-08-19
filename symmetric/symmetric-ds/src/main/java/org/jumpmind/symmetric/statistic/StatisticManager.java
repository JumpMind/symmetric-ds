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
package org.jumpmind.symmetric.statistic;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.INotificationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.util.AppUtils;

public class StatisticManager implements IStatisticManager {

    Map<String, ChannelStats> channelStats;

    INodeService nodeService;

    IStatisticService statisticService;

    INotificationService notificationService;

    IParameterService parameterService;

    IConfigurationService configurationService;

    private static final int NUMBER_OF_PERMITS = 1000;

    Semaphore available = new Semaphore(NUMBER_OF_PERMITS, true);

    public StatisticManager() {
    }

    public void incrementDataRouted(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataRouted(count);
        } finally {
            available.release();
        }
    }

    public void setDataUnRouted(String channelId, long count) {
        getChannelStats(channelId).setDataUnRouted(count);
    }

    public void incrementDataExtracted(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataExtracted(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataBytesExtracted(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesExtracted(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataExtractedErrors(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataExtractedErrors(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataEventInserted(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataEventInserted(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataSent(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataSent(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataBytesSent(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesSent(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataSentErrors(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataSentErrors(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataLoaded(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataLoaded(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataBytesLoaded(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataBytesLoaded(count);
        } finally {
            available.release();
        }
    }

    public void incrementDataLoadedErrors(String channelId, long count) {
        available.acquireUninterruptibly();
        try {
            getChannelStats(channelId).incrementDataLoadedErrors(count);
        } finally {
            available.release();
        }
    }

    public void flush() {
        if (channelStats != null) {
            available.acquireUninterruptibly(NUMBER_OF_PERMITS);
            try {
                Date endTime = new Date();
                for (ChannelStats stats : channelStats.values()) {
                    stats.setEndTime(endTime);
                    statisticService.save(stats);
                }
            } finally {
                available.release(NUMBER_OF_PERMITS);
            }
        }
        reset(true);
    }

    protected void reset(boolean force) {
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
        reset(false);
        ChannelStats stats = channelStats.get(channelId);
        if (stats == null) {
            Node node = nodeService.findIdentity();
            String nodeId = "Unknown";
            if (node != null) {
                nodeId = node.getNodeId();
            }
            stats = new ChannelStats(nodeId, AppUtils.getServerId(), new Date(), null, channelId);
            channelStats.put(channelId, stats);
        }
        return stats;
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
