package org.jumpmind.symmetric.dashboard.services;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.dashboard.model.BatchStatisticsLineItem;
import org.springframework.flex.remoting.RemotingDestination;
import org.springframework.flex.remoting.RemotingInclude;
import org.springframework.stereotype.Service;

@Service("productService")
@RemotingDestination(channels={"defaultRemoteAmfChannel"})
public class BatchStatisticsService implements IBatchStatisticsService {

	private static final Log logger = LogFactory.getLog(BatchStatisticsService.class);
	
	/* (non-Javadoc)
	 * @see org.jumpmind.symmetric.dashboard.services.IBatchStatisticsService#findBatchStatistics(java.util.Date)
	 */
	@RemotingInclude
	public List<BatchStatisticsLineItem> findBatchStatistics(Date d) {
		logger.info("Getting batch statistics for date: " + d);
		ArrayList<BatchStatisticsLineItem> lineItems =
			new ArrayList<BatchStatisticsLineItem>();
		BatchStatisticsLineItem lineItem = new BatchStatisticsLineItem();
		lineItem.setTotalSuccess(10);
		lineItem.setTotalFailures(3);
		lineItem.setTotalBatches(13);
		lineItems.add(lineItem);
		return lineItems;
	}
}