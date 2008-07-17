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
package org.jumpmind.symmetric.model;

import java.math.BigDecimal;

import javax.management.Notification;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.jumpmind.symmetric.statistic.Statistic;

public class StatisticAlertThresholds {

    String statisticName;
    BigDecimal thresholdTotalMax;
    Long thresholdCountMax;
    BigDecimal thresholdTotalMin;
    Long thresholdCountMin;
    BigDecimal thresholdAvgMax;
    BigDecimal thresholdAvgMin;
    static long sequenceNumber = System.currentTimeMillis();

    public StatisticAlertThresholds() {
    }

    public StatisticAlertThresholds(String statisticName, BigDecimal threshholdTotalMax, Long threshholdCountMax,
            BigDecimal threshholdTotalMin, Long threshholdCountMin, BigDecimal threshholdAvgMax,
            BigDecimal threshholdAvgMin) {
        super();
        this.statisticName = statisticName;
        this.thresholdTotalMax = threshholdTotalMax;
        this.thresholdCountMax = threshholdCountMax;
        this.thresholdTotalMin = threshholdTotalMin;
        this.thresholdCountMin = threshholdCountMin;
        this.thresholdAvgMax = threshholdAvgMax;
        this.thresholdAvgMin = threshholdAvgMin;
    }

    public String getStatisticName() {
        return statisticName;
    }

    public void setStatisticName(String statisticName) {
        this.statisticName = statisticName;
    }

    public BigDecimal getThresholdTotalMax() {
        return thresholdTotalMax == null ? BigDecimal.ZERO : thresholdTotalMax;
    }

    public void setThresholdTotalMax(BigDecimal threshholdTotalMax) {
        this.thresholdTotalMax = threshholdTotalMax;
    }

    public Long getThresholdCountMax() {
        return thresholdCountMax == null ? 0l : thresholdCountMax;
    }

    public void setThresholdCountMax(Long threshholdCountMax) {
        this.thresholdCountMax = threshholdCountMax;
    }

    public BigDecimal getThresholdTotalMin() {
        return thresholdTotalMin == null ? BigDecimal.ZERO : thresholdTotalMin;
    }

    public void setThresholdTotalMin(BigDecimal threshholdTotalMin) {
        this.thresholdTotalMin = threshholdTotalMin;
    }

    public Long getThresholdCountMin() {
        return thresholdCountMin == null ? 0l : thresholdCountMin;
    }

    public void setThresholdCountMin(Long threshholdCountMin) {
        this.thresholdCountMin = threshholdCountMin;
    }

    public Notification outsideOfBoundsNotification(Statistic stats) {
        if (stats != null && stats.getName().name().equals(statisticName)) {
            boolean createNotification = false;
            StringBuilder msg = new StringBuilder(statisticName);
            long count = stats.getCount();
            BigDecimal total = stats.getTotal();
            BigDecimal avg = stats.getAverageValue();
            if ((thresholdCountMax != null && thresholdCountMax > 0 && count > thresholdCountMax)
                    || (thresholdCountMin != null && thresholdCountMin > 0 && count < thresholdCountMin)) {
                msg.append(":count=");
                msg.append(count);
                createNotification = true;
            }

            if ((thresholdTotalMax != null && thresholdTotalMax.compareTo(BigDecimal.ZERO) > 0 && total
                    .compareTo(thresholdTotalMax) > 0)
                    || (thresholdTotalMin != null && thresholdTotalMin.compareTo(BigDecimal.ZERO) > 0 && total
                            .compareTo(thresholdTotalMin) < 0)) {
                msg.append(":total=");
                msg.append(total);
                createNotification = true;
            }

            if ((thresholdAvgMax != null && thresholdAvgMax.compareTo(BigDecimal.ZERO) > 0 && avg
                    .compareTo(thresholdAvgMax) > 0)
                    || (thresholdAvgMin != null && thresholdAvgMin.compareTo(BigDecimal.ZERO) > 0 && avg
                            .compareTo(thresholdAvgMin) < 0)) {
                msg.append(":avg=");
                msg.append(avg);
                createNotification = true;
            }

            if (createNotification) {
                return new Notification("SymmetricDS:Alert", stats, sequenceNumber++, System.currentTimeMillis(), msg
                        .toString());
            }

        }
        return null;
    }

    public BigDecimal getThresholdAvgMax() {
        return thresholdAvgMax == null ? BigDecimal.ZERO : thresholdAvgMax;
    }

    public void setThresholdAvgMax(BigDecimal threshholdAvgMax) {
        this.thresholdAvgMax = threshholdAvgMax;
    }

    public BigDecimal getThresholdAvgMin() {
        return thresholdAvgMin == null ? BigDecimal.ZERO : thresholdAvgMin;
    }

    public void setThresholdAvgMin(BigDecimal threshholdAvgMin) {
        this.thresholdAvgMin = threshholdAvgMin;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(statisticName).append(getThresholdTotalMax()).append(
                getThresholdCountMax()).append(getThresholdTotalMin()).append(getThresholdCountMin()).append(
                getThresholdAvgMax()).append(getThresholdAvgMin()).toHashCode();

    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StatisticAlertThresholds == false) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        StatisticAlertThresholds rhs = (StatisticAlertThresholds) obj;
        return new EqualsBuilder().append(statisticName, rhs.statisticName).append(getThresholdTotalMax(),
                rhs.getThresholdTotalMax()).append(getThresholdCountMax(), rhs.getThresholdCountMax()).append(
                getThresholdTotalMin(), rhs.getThresholdTotalMin()).append(getThresholdCountMin(),
                rhs.getThresholdCountMin()).append(getThresholdAvgMax(), rhs.getThresholdAvgMax()).append(
                getThresholdAvgMin(), rhs.getThresholdAvgMin()).isEquals();
    }

}
