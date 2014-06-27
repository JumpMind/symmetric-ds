package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for other threads to
 * process.
 */
public class DataToRouteReader implements Runnable {

	final static ILog log = LogFactory.getLog(DataToRouteReader.class);

	private int fetchSize;

	protected BlockingQueue<Data> dataQueue;

	private DataSource dataSource;

	private RouterContext context;

	private DataRef dataRef;

	private IDataService dataService;

	private IService sql;

	private boolean reading = true;

	private int peekAheadCount;

	private static final int DEFAULT_QUERY_TIMEOUT = 300;

	private int queryTimeout = DEFAULT_QUERY_TIMEOUT;

	public DataToRouteReader(DataSource dataSource, int maxQueueSize,
			IService sql, int fetchSize, RouterContext context,
			DataRef dataRef, IDataService dataService) {
		this(dataSource, maxQueueSize, sql, fetchSize, context, dataRef,
				dataService, DEFAULT_QUERY_TIMEOUT);
	}

	public DataToRouteReader(DataSource dataSource, int peekAheadCount,
			IService sql, int fetchSize, RouterContext context,
			DataRef dataRef, IDataService dataService, int queryTimeout) {
		this.peekAheadCount = peekAheadCount;
		this.dataSource = dataSource;
		this.dataQueue = new LinkedBlockingQueue<Data>(peekAheadCount);
		this.sql = sql;
		this.context = context;
		this.fetchSize = fetchSize;
		this.dataRef = dataRef;
		this.dataService = dataService;
		this.queryTimeout = queryTimeout;
	}

	public Data take() {
		Data data = null;
		try {
			data = dataQueue.poll(queryTimeout == 0 ? 600 : queryTimeout,
					TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.warn(e);
		}

		if (data instanceof EOD) {
			return null;
		} else {
			return data;
		}
	}

	protected String getSql(Channel channel) {
		String select = sql.getSql("selectDataToBatchSql");
		if (!channel.isUseOldDataToRoute()) {
			select = select.replace("d.old_data", "''");
		}
		if (!channel.isUseRowDataToRoute()) {
			select = select.replace("d.row_data", "''");
		}
		if (!channel.isUsePkDataToRoute()) {
			select = select.replace("d.pk_data", "''");
		}
		return select;
	}

	public void run() {
		try {
			JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSource);
			jdbcTemplate.execute(new ConnectionCallback<Integer>() {
				public Integer doInConnection(Connection c)
						throws SQLException, DataAccessException {
					int dataCount = 0;
					PreparedStatement ps = null;
					ResultSet rs = null;
					try {
						String channelId = context.getChannel().getChannelId();
						ps = c.prepareStatement(getSql(context.getChannel()
								.getChannel()), ResultSet.TYPE_FORWARD_ONLY,
								ResultSet.CONCUR_READ_ONLY);
						ps.setQueryTimeout(queryTimeout);
						ps.setFetchSize(fetchSize);
						ps.setString(1, channelId);
						ps.setLong(2, dataRef.getRefDataId());
						long executeTimeInMs = System.currentTimeMillis();
						rs = ps.executeQuery();
						executeTimeInMs = System.currentTimeMillis()
								- executeTimeInMs;
						if (executeTimeInMs > 30000) {
							log.warn("RoutedDataSelectedInTime",
									executeTimeInMs, channelId);
						}

						long maxDataToRoute = context.getChannel()
								.getMaxDataToRoute();
						String lastTransactionId = null;
						List<Data> peekAheadQueue = new ArrayList<Data>(
								peekAheadCount);
						boolean nontransactional = context.getChannel()
								.getBatchAlgorithm().equals("nontransactional");

						boolean moreData = true;
						while (dataCount <= maxDataToRoute
								|| lastTransactionId != null) {
							if (moreData) {
								moreData = fillPeekAheadQueue(peekAheadQueue,
										peekAheadCount, rs);
							}

							if ((lastTransactionId == null || nontransactional)
									&& peekAheadQueue.size() > 0) {
								Data data = peekAheadQueue.remove(0);
								copyToQueue(data);
								dataCount++;
								lastTransactionId = data.getTransactionId();
							} else if (lastTransactionId != null
									&& peekAheadQueue.size() > 0) {
								Iterator<Data> datas = peekAheadQueue
										.iterator();
								int dataWithSameTransactionIdCount = 0;
								while (datas.hasNext()) {
									Data data = datas.next();
									if (lastTransactionId.equals(data
											.getTransactionId())) {
										dataWithSameTransactionIdCount++;
										datas.remove();
										copyToQueue(data);
										dataCount++;
									}
								}

								if (dataWithSameTransactionIdCount == 0) {
									lastTransactionId = null;
								}

							} else if (peekAheadQueue.size() == 0) {
								// we've reached the end of the result set
								break;
							}
						}

						return dataCount;

					} finally {

						JdbcUtils.closeResultSet(rs);
						JdbcUtils.closeStatement(ps);
						rs = null;
						ps = null;

						long ts = System.currentTimeMillis();

						boolean done = false;
						do {
							done = dataQueue.offer(new EOD());
							AppUtils.sleep(50);
						} while (!done && reading);

						context.incrementStat(System.currentTimeMillis() - ts,
								RouterContext.STAT_ENQUEUE_EOD_MS);

						reading = false;

					}
				}
			});
		} catch (Throwable ex) {
			log.error(ex);
		}
	}

	protected boolean fillPeekAheadQueue(List<Data> peekAheadQueue,
			int peekAheadCount, ResultSet rs) throws SQLException {
		boolean moreData = true;
		int toRead = peekAheadCount - peekAheadQueue.size();
		int dataCount = 0;
		long ts = System.currentTimeMillis();
		while (reading && dataCount < toRead) {
			if (rs.next()) {
				if (process(rs)) {
					Data data = dataService.readData(rs);
					context.setLastDataIdForTransactionId(data);
					peekAheadQueue.add(data);
					dataCount++;
					context.incrementStat(System.currentTimeMillis() - ts,
							RouterContext.STAT_READ_DATA_MS);
				} else {
					context.incrementStat(System.currentTimeMillis() - ts,
							RouterContext.STAT_REREAD_DATA_MS);
				}

				ts = System.currentTimeMillis();
			} else {
				moreData = false;
				break;
			}

		}
		return moreData;
	}

	protected boolean process(ResultSet rs) throws SQLException {
		return StringUtils.isBlank(rs.getString(13));
	}

	protected void copyToQueue(Data data) {
		long ts = System.currentTimeMillis();
		while (!dataQueue.offer(data) && reading) {
			AppUtils.sleep(50);
		}
		context.incrementStat(System.currentTimeMillis() - ts,
				RouterContext.STAT_ENQUEUE_DATA_MS);
	}

	public boolean isReading() {
		return reading;
	}

	public void setReading(boolean reading) {
		this.reading = reading;
	}

	public BlockingQueue<Data> getDataQueue() {
		return dataQueue;
	}

	class EOD extends Data {

	}
}
