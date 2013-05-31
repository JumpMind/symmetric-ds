package org.jumpmind.symmetric.statistic;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChannelStatsByPeriodMap extends AbstractStatsByPeriodMap<Map<String,ChannelStats>,ChannelStats> {

    private static final long serialVersionUID = 1L;

    public ChannelStatsByPeriodMap(Date start, Date end, List<ChannelStats> list,
            int periodSizeInMinutes) {
        super(start, end, list, periodSizeInMinutes);
    }

    @Override
    protected void add(Date periodStart, ChannelStats stat) {
        Map<String, ChannelStats> map = get(periodStart);
        if (map == null) {
            map = new HashMap<String, ChannelStats>();
            put(periodStart, map);
        }
        ChannelStats existing = map.get(stat.getChannelId());
        if (existing == null) {
            map.put(stat.getChannelId(), stat);
        } else {
            existing.add(stat);
        }        
    }
    
    @Override
    protected void addBlank(Date periodStart) {
        put(periodStart, new HashMap<String, ChannelStats>());
        
    }
}
