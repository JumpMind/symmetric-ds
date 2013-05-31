package org.jumpmind.symmetric.statistic;

import java.util.Date;
import java.util.List;

public class HostStatsByPeriodMap extends AbstractStatsByPeriodMap<HostStats, HostStats> {

    private static final long serialVersionUID = 1L;

    public HostStatsByPeriodMap(Date start, Date end, List<HostStats> list, int periodSizeInMinutes) {
        super(start, end, list, periodSizeInMinutes);
    }

    @Override
    protected void add(Date periodStart, HostStats stat) {
        put(periodStart, stat);
    }

    @Override
    protected void addBlank(Date periodStart) {
        put(periodStart, new HostStats());
    }
}
