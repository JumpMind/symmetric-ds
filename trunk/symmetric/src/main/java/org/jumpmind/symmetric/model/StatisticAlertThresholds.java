package org.jumpmind.symmetric.model;

import java.math.BigDecimal;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class StatisticAlertThresholds {

    String statisticName;
    BigDecimal threshholdTotalMax;
    Long threshholdCountMax;
    BigDecimal threshholdTotalMin;
    Long threshholdCountMin;

    public StatisticAlertThresholds() {
    }

    public StatisticAlertThresholds(String statisticName, BigDecimal threshholdTotalMax, Long threshholdCountMax,
            BigDecimal threshholdTotalMin, Long threshholdCountMin) {
        super();
        this.statisticName = statisticName;
        this.threshholdTotalMax = threshholdTotalMax;
        this.threshholdCountMax = threshholdCountMax;
        this.threshholdTotalMin = threshholdTotalMin;
        this.threshholdCountMin = threshholdCountMin;
    }

    public String getStatisticName() {
        return statisticName;
    }

    public void setStatisticName(String statisticName) {
        this.statisticName = statisticName;
    }

    public BigDecimal getThreshholdTotalMax() {
        return threshholdTotalMax;
    }

    public void setThreshholdTotalMax(BigDecimal threshholdTotalMax) {
        this.threshholdTotalMax = threshholdTotalMax;
    }

    public Long getThreshholdCountMax() {
        return threshholdCountMax;
    }

    public void setThreshholdCountMax(Long threshholdCountMax) {
        this.threshholdCountMax = threshholdCountMax;
    }

    public BigDecimal getThreshholdTotalMin() {
        return threshholdTotalMin;
    }

    public void setThreshholdTotalMin(BigDecimal threshholdTotalMin) {
        this.threshholdTotalMin = threshholdTotalMin;
    }

    public Long getThreshholdCountMin() {
        return threshholdCountMin;
    }

    public void setThreshholdCountMin(Long threshholdCountMin) {
        this.threshholdCountMin = threshholdCountMin;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(statisticName).append(threshholdTotalMax).append(threshholdCountMax)
                .append(threshholdTotalMin).append(threshholdCountMin).toHashCode();

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
        return new EqualsBuilder().append(statisticName, rhs.statisticName).append(
                threshholdTotalMax, rhs.threshholdTotalMax).append(threshholdCountMax, rhs.threshholdCountMax).append(
                threshholdTotalMin, rhs.threshholdTotalMin).append(threshholdCountMin, rhs.threshholdCountMin)
                .isEquals();
    }

}
