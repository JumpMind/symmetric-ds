package org.jumpmind.symmetric.dashboard.services;

import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.dashboard.model.BatchStatisticsLineItem;

public interface IBatchStatisticsService {

	public List<BatchStatisticsLineItem> findBatchStatistics(Date d);
}
