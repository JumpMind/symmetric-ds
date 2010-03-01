package org.jumpmind.symmetric.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.DataSource;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataRef;
import org.jumpmind.symmetric.service.IDataService;
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

    private Map<String, String> sql;

    private DataSource dataSource;

    private RouterContext context;

    private DataRef dataRef;

    private IDataService dataService;

    private boolean reading = true;

    private int maxQueueSize;

    public DataToRouteReader(DataSource dataSource, int maxQueueSize, Map<String, String> sql,
            int fetchSize, RouterContext context, DataRef dataRef, IDataService dataService) {
        this.maxQueueSize = maxQueueSize;
        this.dataSource = dataSource;
        this.dataQueue = new LinkedBlockingQueue<Data>(maxQueueSize);
        this.sql = sql;
        this.context = context;
        this.fetchSize = fetchSize;
        this.dataRef = dataRef;
        this.dataService = dataService;
    }

    public Data take() {
        Data data;
        try {
            data = dataQueue.take();
        } catch (InterruptedException e) {
            log.error(e);
            return null;
        }
        if (data instanceof EOD) {
            return null;
        } else {
            return data;
        }
    }
    
    protected String getSql (Channel channel) {
        String select = sql.get("selectDataToBatchSql");
        if (!channel.isUseOldDataToRoute()) {
            select = select.replace("d.old_data", "null");
        }
        if (!channel.isUseRowDataToRoute()) {
            select = select.replace("d.row_data", "null");
        }
        return select;
    }

    public void run() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(this.dataSource);
        jdbcTemplate.execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                int dataCount = 0;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    String channelId = context.getChannel().getChannelId();
                    ps = c.prepareStatement(getSql(context.getChannel().getChannel()),
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
                            for (Data data : memQueue) {
                                dataQueue.put(data);
                            }
                            toRead = maxQueueSize - dataQueue.size();
                            memQueue = new ArrayList<Data>(toRead);
                        } else {
                            toRead--;
                        }

                        context.incrementStat(System.currentTimeMillis() - ts,
                                RouterContext.STAT_ENQUEUE_DATA_MS);
                        
                        ts = System.currentTimeMillis();
                    }

                    for (Data data : memQueue) {
                        dataQueue.put(data);
                    }

                    return dataCount;
                } catch (InterruptedException ex) {
                    log.error(ex);
                    throw new RuntimeException(ex);
                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(ps);
                    reading = false;
                    try {
                        dataQueue.put(new EOD());
                    } catch (InterruptedException ex) {
                        log.error(ex);
                    }
                    rs = null;
                    ps = null;
                }
            }
        });
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
