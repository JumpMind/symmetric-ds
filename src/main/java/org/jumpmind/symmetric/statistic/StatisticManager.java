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

import javax.management.Notification;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.INotificationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IStatisticService;

public class StatisticManager implements IStatisticManager {

    Map<String, Statistic> statistics;

    INodeService nodeService;

    IStatisticService statisticService;

    INotificationService notificationService;

    IParameterService parameterService;
    
    Date lastCaptureEndTime;

    synchronized public void init() {
        if (statistics == null) {
            refresh(new Date());
        }
    }

    synchronized public void flush() {
        Date captureEndTime = new Date();
        if (statistics != null) {
            statisticService.save(statistics.values(), captureEndTime);
            publishAlerts();
        }
        refresh(captureEndTime);
    }

    /**
     * Compare the statistics we have cached against the configured statistic
     * alert thresholds and publish alerts if we fall outside the range.
     */
    synchronized protected void publishAlerts() {
        if (parameterService.is(ParameterConstants.STATISTIC_THRESHOLD_ALERTS_ENABLED) && statistics != null) {
            List<StatisticAlertThresholds> thresholds = statisticService.getAlertThresholds();
            if (thresholds != null) {
                for (StatisticAlertThresholds statisticAlertThresholds : thresholds) {
                    String name = statisticAlertThresholds.getStatisticName();
                    if (name != null) {
                        Notification event = statisticAlertThresholds.outsideOfBoundsNotification(statistics.get(name));
                        if (event != null) {
                            notificationService.sendNotification(event);
                        }
                    }
                }
            }
        }
    }

    synchronized protected void refresh(Date lastCaptureEndTime) {
        if (statistics == null) {
            statistics = new HashMap<String, Statistic>();
        }
        this.lastCaptureEndTime = lastCaptureEndTime;
        statistics.clear();
    }

    public Statistic getStatistic(String statisticName) {
        this.init();
        Statistic statistic = statistics.get(statisticName);
        if (statistic == null) {
            Node node = nodeService.findIdentity();
            String nodeId = "Unknown";
            if (node != null) {
                nodeId = node.getNodeId();
            }
            statistic = new Statistic(statisticName, nodeId, lastCaptureEndTime == null ? new Date()
            : lastCaptureEndTime);
            statistics.put(statisticName, statistic);
        }
        return statistic;
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
}
