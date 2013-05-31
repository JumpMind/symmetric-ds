package org.jumpmind.symmetric.statistic;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.lang.time.DateUtils;

abstract public class AbstractStatsByPeriodMap<T,M extends AbstractNodeHostStats> extends TreeMap<Date, T> {

    private static final long serialVersionUID = 1L;

    public AbstractStatsByPeriodMap(Date start, Date end, List<M> list, int periodSizeInMinutes) {
        Iterator<M> stats = list.iterator();
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(start);
        startCal.set(Calendar.SECOND, 0);
        startCal.set(Calendar.MILLISECOND, 0);
        startCal.set(Calendar.MINUTE, round(startCal.get(Calendar.MINUTE)));
        Date periodStart = startCal.getTime();
        Date periodEnd = DateUtils.add(periodStart, Calendar.MINUTE, periodSizeInMinutes);
        M stat = null;
        while (periodStart.before(end)) {
            if (stat == null && stats.hasNext()) {
                stat = stats.next();
            }
            if (stat != null
                    && (periodStart.equals(stat.getStartTime()) || periodStart.before(stat
                            .getStartTime())) && periodEnd.after(stat.getStartTime())) {
                add(periodStart, stat);
                stat = null;
            } else {
                if (stat != null && stat.getStartTime().before(periodStart)) {
                    stat = null;
                }
                if (!containsKey(periodStart)) {
                    addBlank(periodStart);
                }
                periodStart = periodEnd;
                periodEnd = DateUtils.add(periodStart, Calendar.MINUTE, periodSizeInMinutes);
            }
        }
    }
    
    abstract protected void addBlank(Date periodStart);
    
    abstract protected void add(Date periodStart, M stat);
    
    protected int round(int value) {
        return 5 * new BigDecimal((double) value / 5d).setScale(2, BigDecimal.ROUND_HALF_DOWN)
                .intValue();
    }
}
