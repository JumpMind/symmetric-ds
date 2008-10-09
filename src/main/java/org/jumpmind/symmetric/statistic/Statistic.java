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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Statistic {

    private StatisticName name;

    private String nodeId;

    private Date captureStartTimeMs;

    private BigDecimal total;

    private long count;

    private static Map<StatisticName, BigDecimal> lifeTimeTotal = new HashMap<StatisticName, BigDecimal>(StatisticName
            .values().length);

    private static Map<StatisticName, Long> lifeTimeCount = new HashMap<StatisticName, Long>(
            StatisticName.values().length);

    static {
        StatisticName[] allStatistics = StatisticName.values();
        for (StatisticName statisticName : allStatistics) {
            lifeTimeCount.put(statisticName, new Long(0));
            lifeTimeTotal.put(statisticName, BigDecimal.ZERO);
        }
    }

    public Statistic(StatisticName name, String nodeId) {
        this(name, nodeId, new Date());
    }

    public Statistic(StatisticName name, String nodeId, Date startTime) {
        this.name = name;
        this.nodeId = nodeId;
        this.captureStartTimeMs = startTime;
        this.total = BigDecimal.ZERO;
    }

    public StatisticName getName() {
        return name;
    }

    public String getNodeId() {
        return nodeId;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public BigDecimal getLifetimeTotal() {
        return lifeTimeTotal.get(name);
    }

    public long getLifetimeCount() {
        return lifeTimeCount.get(name);
    }

    public BigDecimal getLifetimeAverageValue() {
        return getAverageValue(getLifetimeTotal(), getLifetimeCount());
    }

    public BigDecimal getAverageValue() {
        return getAverageValue(this.total, this.count);
    }

    private BigDecimal getAverageValue(BigDecimal total, long count) {
        if (total != null && total.compareTo(BigDecimal.ZERO) > 0 && count > 0) {
            return total.divide(new BigDecimal(count), RoundingMode.HALF_UP);
        } else {
            return BigDecimal.ZERO;
        }
    }

    public void increment() {
        add(1, 1);
    }

    public void add(int v) {
        add(v, 1);
    }

    public void add(BigDecimal v) {
        add(v, 1);
    }

    public void add(long v) {
        add(v, 1);
    }

    public void add(long v, long count) {
        synchronized (name) {
            this.total = this.total.add(new BigDecimal(v));
            this.count += count;
            lifeTimeCount.put(name, new Long(lifeTimeCount.get(name) + count));
            lifeTimeTotal.put(name, lifeTimeTotal.get(name).add(new BigDecimal(v)));
        }
    }

    public void add(int v, long count) {
        synchronized (name) {
            this.total = this.total.add(new BigDecimal(v));
            this.count += count;
            lifeTimeCount.put(name, new Long(lifeTimeCount.get(name) + count));
            lifeTimeTotal.put(name, lifeTimeTotal.get(name).add(new BigDecimal(v)));
        }
    }

    public void add(BigDecimal v, long count) {
        synchronized (name) {
            this.total = this.total.add(v);
            this.count += count;
            lifeTimeCount.put(name, new Long(lifeTimeCount.get(name) + count));
            lifeTimeTotal.put(name, lifeTimeTotal.get(name).add(v));
        }
    }

    public Date getCaptureStartTimeMs() {
        return captureStartTimeMs;
    }

    public long getCount() {
        return count;
    }

}
