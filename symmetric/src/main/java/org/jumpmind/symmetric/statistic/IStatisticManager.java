package org.jumpmind.symmetric.statistic;

public interface IStatisticManager {
    public Statistic getStatistic(StatisticName name);
    public void flush();
}
