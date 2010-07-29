package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;
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

    private ISqlProvider sqlProvider;

    private DataSource dataSource;

    private RouterContext context;

    private DataRef dataRef;

    private IDataService dataService;

    private boolean reading = true;

    private int maxQueueSize;
    
    private static final int DEFAULT_QUERY_TIMEOUT = 300;

    private int queryTimeout = DEFAULT_QUERY_TIMEOUT;      

    public DataToRouteReader(DataSource dataSource, int queryTimeout, int maxQueueSize, ISqlProvider sqlProvider,
            int fetchSize, RouterContext context, DataRef dataRef, IDataService dataService) {
        this.maxQueueSize = maxQueueSize;
        this.dataSource = dataSource;
        this.dataQueue = new LinkedBlockingQueue<Data>(maxQueueSize);
        this.sqlProvider = sqlProvider;
        this.context = context;
        this.fetchSize = fetchSize;
        this.dataRef = dataRef;
        this.queryTimeout = queryTimeout;
        this.dataService = dataService;
    }

    public Data take() {
        Data data = null;
        try {
            data = dataQueue.poll(queryTimeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn(e);
        }

        if (data instanceof EOD) {
            return null;
        } else {
            return data;
        }
    }
    
    protected String getSql (Channel channel) {
        String select = sqlProvider.getSql("selectDataToBatchSql");
        if (!channel.isUseOldDataToRoute()) {
            select = select.replace("d.old_data", "''");
        }
        if (!channel.isUseRowDataToRoute()) {
            select = select.replace("d.row_data", "''");
        }
        return select;
    }

    public void run() {
        try {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSource);
            jdbcTemplate.execute(new ConnectionCallback<Integer>() {
                public Integer doInConnection(Connection c) throws SQLException,
                        DataAccessException {
                    int dataCount = 0;
                    PreparedStatement ps = null;
                    ResultSet rs = null;
                    try {
                        String channelId = context.getChannel().getChannelId();
                        ps = c.prepareStatement(getSql(context.getChannel().getChannel()),
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                        ps.setQueryTimeout(queryTimeout);
                        ps.setFetchSize(fetchSize);
                        ps.setString(1, channelId);
                        ps.setLong(2, dataRef.getRefDataId());
                        long executeTimeInMs = System.currentTimeMillis();
                        rs = ps.executeQuery();
                        executeTimeInMs = System.currentTimeMillis() - executeTimeInMs;
                        if (executeTimeInMs > 30000) {
                            log.warn("RoutedDataSelectedInTime", executeTimeInMs, channelId);
                        }

                        int toRead = maxQueueSize - dataQueue.size();
                        List<Data> memQueue = new ArrayList<Data>(toRead);
                        long ts = System.currentTimeMillis();
                        while (rs.next() && reading) {

                            if (rs.getString(13) == null) {
                                Data data = dataService.readData(rs);
                                context.setLastDataIdForTransactionId(data);
                                memQueue.add(data);
                                dataCount++;
                            }
                            context.incrementStat(System.currentTimeMillis() - ts,
                                    RouterContext.STAT_READ_DATA_MS);

                            ts = System.currentTimeMillis();

                            if (toRead == 0) {
                                copyToQueue(memQueue);
                                toRead = maxQueueSize - dataQueue.size();
                                memQueue = new ArrayList<Data>(toRead);
                            } else {
                                toRead--;
                            }

                            context.incrementStat(System.currentTimeMillis() - ts,
                                    RouterContext.STAT_ENQUEUE_DATA_MS);

                            ts = System.currentTimeMillis();
                        }

                        ts = System.currentTimeMillis();

                        copyToQueue(memQueue);

                        context.incrementStat(System.currentTimeMillis() - ts,
                                RouterContext.STAT_ENQUEUE_DATA_MS);

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
    
    protected void copyToQueue(List<Data> memQueue) {
        while (memQueue.size() > 0 && reading) {
        	Data d = memQueue.get(0);
            if (dataQueue.offer(d)) {
            	memQueue.remove(0);
            } else {
            	AppUtils.sleep(50);
            }
        }
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
