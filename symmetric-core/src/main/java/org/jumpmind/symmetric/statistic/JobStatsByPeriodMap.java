package org.jumpmind.symmetric.statistic;

import java.util.Date;
import java.util.List;

public class JobStatsByPeriodMap extends AbstractStatsByPeriodMap<JobStats, JobStats> {
    private static final long serialVersionUID = 1L;

    public JobStatsByPeriodMap(Date start, Date end, List<JobStats> list, int periodSizeInMinutes) {
        super(start, end, list, periodSizeInMinutes);
    }

    @Override
    protected void add(Date periodStart, JobStats stat) {
        put(periodStart, stat);
    }

    @Override
    protected void addBlank(Date periodStart) {
        put(periodStart, new JobStats());
    }
}
