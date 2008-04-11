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
import java.util.Date;

public class Statistic {

    private StatisticName name;

    private String nodeId;

    private Date captureStartTimeMs;

    private BigDecimal total;
    
    private long count;

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
    
    public BigDecimal getAverageValue() {
        if (total != null && count > 0) {
            return total.divide(new BigDecimal(count));
        } else {
            return BigDecimal.ZERO;
        }
    }
    
    public void add(int v) {
        add(v,1);
    }

    public void add(BigDecimal v) {
        add(v,1);
    }
    
    public void add(long v) {
        add(v,1);
    }

    public void add(long v, int count) {
        this.total = this.total.add(new BigDecimal(v));
        this.count += this.count;
    }

    public void add(int v, int count) {
        this.total = this.total.add(new BigDecimal(v));
        this.count += this.count;
    }

    public void add(BigDecimal v, int count) {
        this.total = this.total.add(v);
        this.count += this.count;
    }

    public Date getCaptureStartTimeMs() {
        return captureStartTimeMs;
    }

    public long getCount() {
        return count;
    }

}
