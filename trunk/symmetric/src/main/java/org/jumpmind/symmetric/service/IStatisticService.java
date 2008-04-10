package org.jumpmind.symmetric.service;

import java.util.Collection;
import java.util.Date;

import org.jumpmind.symmetric.statistic.Statistic;

public interface IStatisticService {

    public void save(Collection<Statistic> stats, Date captureEndTime);
}
