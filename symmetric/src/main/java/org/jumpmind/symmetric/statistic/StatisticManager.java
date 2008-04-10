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
        refresh(new Date());
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
        return statistics.get(name);
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setStatisticService(IStatisticService statisticService) {
        this.statisticService = statisticService;
    }
}
