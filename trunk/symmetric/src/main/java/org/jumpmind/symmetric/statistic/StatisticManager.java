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
import java.util.Map;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IStatisticService;

public class StatisticManager implements IStatisticManager {

    Map<StatisticName, Statistic> statistics;

    INodeService nodeService;

    IStatisticService statisticService;

    synchronized public void init() {
        if (statistics == null) {
            refresh(new Date());
        }
    }

    synchronized public void flush() {
        Date captureEndTime = new Date();
        if (statistics != null) {
            statisticService.save(statistics.values(), captureEndTime);
        }
        refresh(captureEndTime);
    }

    synchronized protected void refresh(Date lastCaptureEndTime) {
        if (statistics == null) {
            statistics = new HashMap<StatisticName, Statistic>();
        }

        statistics.clear();

        Node node = nodeService.findIdentity();

        if (node != null) {
            String nodeId = node.getNodeId();
            StatisticName[] all = StatisticName.values();
            for (StatisticName statisticName : all) {
                statistics.put(statisticName, new Statistic(statisticName, nodeId,
                        lastCaptureEndTime == null ? new Date() : lastCaptureEndTime));
            }
        }
    }

    public Statistic getStatistic(StatisticName name) {
        this.init();
        return statistics.get(name);
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setStatisticService(IStatisticService statisticService) {
        this.statisticService = statisticService;
    }
}
