/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.route;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.jumpmind.symmetric.common.Constants.LOG_PROCESS_SUMMARY_THRESHOLD;

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.ProcessStatus;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for another thread
 * to process.
 */
public class DataGapRouteReader implements IDataToRouteReader {

    protected final static Logger log = LoggerFactory.getLogger(DataGapRouteReader.class);

    protected List<DataGap> dataGaps;

    protected DataGap currentGap;

    protected BlockingQueue<Data> dataQueue;

    protected ChannelRouterContext context;

    protected ISymmetricEngine engine;

    protected volatile boolean reading = true;
    
    protected int peekAheadCount = 1000;
    
    protected int takeTimeout;
    
    protected ProcessInfo processInfo;
    
    protected double percentOfHeapToUse = .5;
    
    protected long peekAheadSizeInBytes = 0;
    
    protected boolean finishTransactionMode = false;

    protected boolean isEachGapQueried;
    
    protected boolean isOracleNoOrder;
    
    protected boolean isSortInMemory;

    protected String lastTransactionId = null;
    
    protected static Map<String, Boolean> lastSelectUsedGreaterThanQueryByEngineName = new HashMap<String, Boolean>(); 
    
    long lastStatsPrintOutBaselineInMs = System.currentTimeMillis();

    public DataGapRouteReader(ChannelRouterContext context, ISymmetricEngine engine) {
        this.engine = engine;
        IParameterService parameterService = engine.getParameterService();
        this.peekAheadCount = parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW);
        this.percentOfHeapToUse = (double)parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_MEMORY_THRESHOLD)/(double)100;
        this.takeTimeout = engine.getParameterService().getInt(
                ParameterConstants.ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS, 330);
        this.isOracleNoOrder = parameterService.is(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER, false);
        this.isSortInMemory = parameterService.is(ParameterConstants.ROUTING_DATA_READER_INTO_MEMORY_ENABLED, false);
        if (parameterService.is(ParameterConstants.SYNCHRONIZE_ALL_JOBS)) {
            /* there will not be a separate thread to read a blocked queue so make sure the queue is big enough that it can be filled */
            this.dataQueue = new LinkedBlockingQueue<Data>();
        } else {
            this.dataQueue = new LinkedBlockingQueue<Data>(peekAheadCount);
        }
        this.context = context;
        
        String engineName = parameterService.getEngineName();
        if (lastSelectUsedGreaterThanQueryByEngineName.get(engineName) == null) {
            lastSelectUsedGreaterThanQueryByEngineName.put(engineName, Boolean.FALSE);
        }
        this.dataGaps = new ArrayList<DataGap>(context.getDataGaps());
    }

    public void run() {
        try {
            execute();
        } catch (Throwable ex) {
            log.error("", ex);
        }
    }

    protected void execute() {    
        ISymmetricDialect symmetricDialect = engine.getSymmetricDialect();
        ISqlReadCursor<Data> cursor = null;
        processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(engine.getNodeService().findIdentityNodeId(), null,
                        ProcessType.ROUTER_READER));
        processInfo.setCurrentChannelId(context.getChannel().getChannelId());
        try {
            boolean transactional = !context.getChannel().getBatchAlgorithm()
                    .equals(NonTransactionalBatchAlgorithm.NAME)
                    || !symmetricDialect.supportsTransactionId();
            
            processInfo.setStatus(ProcessStatus.QUERYING);
            cursor = prepareCursor();
            processInfo.setStatus(ProcessStatus.EXTRACTING);            
            
            if (transactional) {
                executeTransactional(cursor);
            } else {
                executeNonTransactional(cursor);
            }            
            
            processInfo.setStatus(ProcessStatus.OK);
        } catch (Throwable ex) {
            processInfo.setStatus(ProcessStatus.ERROR);
            String msg = "";
            if (engine.getDatabasePlatform().getName().startsWith(DatabaseNamesConstants.FIREBIRD)
                    && isNotBlank(ex.getMessage())
                    && ex.getMessage().contains(
                            "arithmetic exception, numeric overflow, or string truncation")) {
                msg = "There is a good chance that the truncation error you are receiving is because contains_big_lobs on the '"
                        + context.getChannel().getChannelId()
                        + "' channel needs to be turned on.  Firebird casts to varchar when this setting is not turned on and the data length has most likely exceeded the 10k row size";
            }
            log.error(msg, ex);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            copyToQueue(new EOD());
            reading = false;
        }

    }
    
    protected void executeTransactional(ISqlReadCursor<Data> cursor) throws Exception {
        long maxPeekAheadSizeInBytes = (long)(Runtime.getRuntime().maxMemory() * percentOfHeapToUse);
        int lastPeekAheadIndex = 0;
        int dataCount = 0;
        long maxDataToRoute = context.getChannel().getMaxDataToRoute();
        List<Data> peekAheadQueue = new ArrayList<Data>(peekAheadCount);

        boolean moreData = true;
        while (dataCount < maxDataToRoute || (lastTransactionId != null)) {
            if (moreData && (lastTransactionId != null || peekAheadQueue.size() == 0)) {
                moreData = fillPeekAheadQueue(peekAheadQueue, peekAheadCount, cursor);
            }                

            int dataWithSameTransactionIdCount = 0;
            
            while (peekAheadQueue.size() > 0 && lastTransactionId == null &&
                    dataCount < maxDataToRoute) {
                Data data = peekAheadQueue.remove(0);
                copyToQueue(data);
                dataCount++;
                processInfo.incrementCurrentDataCount();
                processInfo.setCurrentTableName(data.getTableName());
                lastTransactionId = data.getTransactionId();
                dataWithSameTransactionIdCount++;
            }

            if (lastTransactionId != null && peekAheadQueue.size() > 0) {
                Iterator<Data> datas = peekAheadQueue.iterator();
                int index = 0;
                while (datas.hasNext()) {
                    Data data = datas.next();                        
                    if (lastTransactionId.equals(data.getTransactionId())) {
                        dataWithSameTransactionIdCount++;
                        datas.remove();
                        copyToQueue(data);
                        dataCount++;
                        processInfo.incrementCurrentDataCount();
                        processInfo.setCurrentTableName(data.getTableName());
                        lastPeekAheadIndex = index;
                    } else {
                        index++;
                    }
                    
                }

                if (dataWithSameTransactionIdCount == 0 || peekAheadQueue.size()-lastPeekAheadIndex > peekAheadCount) {
                    lastTransactionId = null;
                    lastPeekAheadIndex = 0;
                }

            } 
            
            if (!moreData && peekAheadQueue.size() == 0) {
                // we've reached the end of the result set
                break;
            } else if (peekAheadSizeInBytes >= maxPeekAheadSizeInBytes) {
                log.info("The peek ahead queue has reached its max size of {} bytes.  Finishing reading the current transaction", peekAheadSizeInBytes);
                finishTransactionMode = true;
                peekAheadQueue.clear();                    
            }
        }
    }
    
    
    protected void executeNonTransactional(ISqlReadCursor<Data> cursor) throws Exception {
        long maxDataToRoute = context.getChannel().getMaxDataToRoute();
        List<Data> peekAheadQueue = new ArrayList<Data>(peekAheadCount);
        int dataCount = 0;
        while (dataCount < maxDataToRoute) {
            fillPeekAheadQueue(peekAheadQueue, peekAheadCount, cursor);
            
            if (peekAheadQueue.size() > 0) {
                while (peekAheadQueue.size() > 0 && dataCount < maxDataToRoute) {
                    Data data = peekAheadQueue.remove(0);
                    copyToQueue(data);
                    dataCount++;
                    processInfo.incrementCurrentDataCount();
                    processInfo.setCurrentTableName(data.getTableName());
                }
                
            } else {
                 break;
            }
        }
    }    

    protected boolean process(Data data) {
        long dataId = data.getDataId();
        boolean okToProcess = false;
        if (!finishTransactionMode
                || (lastTransactionId != null && finishTransactionMode && lastTransactionId
                        .equals(data.getTransactionId()))) {
            if (isOracleNoOrder) {
                if (isEachGapQueried) {
                    okToProcess = true;
                } else {
                    okToProcess = isInDataGap(dataId);
                }
            } else {
                while (!okToProcess && currentGap != null && dataId >= currentGap.getStartId()) {
                    if (dataId <= currentGap.getEndId()) {
                        okToProcess = true;
                    } else {
                        // past current gap. move to next gap
                        if (dataGaps.size() > 0) {
                            currentGap = dataGaps.remove(0);
                        } else {
                            currentGap = null;
                        }
                    }
                }
            }
        }
        return okToProcess;
    }
    
    protected boolean isInDataGap(long dataId) {
        // binary search algorithm
        int start = 0;
        int end = dataGaps.size() - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            DataGap midGap = dataGaps.get(mid);
            if (dataId >= midGap.getStartId() && dataId <= midGap.getEndId()) {
                return true;
            }
            if (dataId< midGap.getStartId()) {
                end = mid - 1;
            } else {
                start = mid + 1;
            }
        }
        return false;
    }    

    public Data take() throws InterruptedException {
        Data data = null;
        do {
            data = dataQueue.poll(takeTimeout, TimeUnit.SECONDS);

            if (data == null && !reading) {
                throw new SymmetricException("The read of the data to route queue has timed out");
            } else if (data instanceof EOD) {
                data = null;
                break;
            } 
            
        } while (data == null && reading);
        
        return data;
    }

    protected ISqlReadCursor<Data> prepareCursor() {
        IParameterService parameterService = engine.getParameterService();
        int numberOfGapsToQualify = parameterService.getInt(
                ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);
        
        int maxGapsBeforeGreaterThanQuery = parameterService.getInt(ParameterConstants.ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY, 100);
                
        boolean useGreaterThanDataId = false;
        if (maxGapsBeforeGreaterThanQuery > 0 && this.dataGaps.size() > maxGapsBeforeGreaterThanQuery) {
            useGreaterThanDataId = true;
        }

        isEachGapQueried = !useGreaterThanDataId && this.dataGaps.size() <= numberOfGapsToQualify;
        String channelId = context.getChannel().getChannelId();

        String sql = null;
        
        Boolean lastSelectUsedGreaterThanQuery = lastSelectUsedGreaterThanQueryByEngineName.get(parameterService.getEngineName());
        if (lastSelectUsedGreaterThanQuery == null) {
            lastSelectUsedGreaterThanQuery = Boolean.FALSE;
        }
        
        if (useGreaterThanDataId) {            
            sql = getSql("selectDataUsingStartDataId", context.getChannel().getChannel());
            if (!lastSelectUsedGreaterThanQuery) {
                log.info("Switching to select from the data table where data_id >= start gap because there were {} gaps found "
                        + "which was more than the configured threshold of {}", dataGaps.size(), maxGapsBeforeGreaterThanQuery);
                lastSelectUsedGreaterThanQueryByEngineName.put(parameterService.getEngineName(), Boolean.TRUE);
            }
        } else {
            sql = qualifyUsingDataGaps(dataGaps, numberOfGapsToQualify,
                    getSql("selectDataUsingGapsSql", context.getChannel().getChannel()));            
            if (lastSelectUsedGreaterThanQuery) {
                log.info("Switching to select from the data table where data_id between gaps");
                lastSelectUsedGreaterThanQueryByEngineName.put(parameterService.getEngineName(), Boolean.FALSE);
            }            
        }
        
        if (!isSortInMemory) {
            if (isOracleNoOrder) {
                sql = String.format("%s %s", sql, engine.getRouterService().getSql("orderByCreateTime"));
            } else if (parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, true)) {
                sql = String.format("%s %s", sql, engine.getRouterService().getSql("orderByDataId"));
            }
        }

        ISqlTemplate sqlTemplate = engine.getSymmetricDialect().getPlatform().getSqlTemplate();
        Object[] args = null;
        int[] types = null;
        
        int dataIdSqlType = engine.getSymmetricDialect().getSqlTypeForIds();
        if (useGreaterThanDataId) {
            args = new Object[] { channelId, dataGaps.get(0).getStartId() };
            types = new int[] { Types.VARCHAR, dataIdSqlType };
        } else {
            int numberOfArgs = 1 + 2 * (numberOfGapsToQualify < dataGaps.size() ? numberOfGapsToQualify
                    : dataGaps.size());
            args = new Object[numberOfArgs];
            types = new int[numberOfArgs];
            args[0] = channelId;
            types[0] = Types.VARCHAR;

            for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
                DataGap gap = dataGaps.get(i);
                args[i * 2 + 1] = gap.getStartId();
                types[i * 2 + 1] = dataIdSqlType;
                if ((i + 1) == numberOfGapsToQualify && (i + 1) < dataGaps.size()) {
                    /*
                     * there were more gaps than we are going to use in the SQL.
                     * use the last gap as the end data id for the last range
                     */
                    args[i * 2 + 2] = dataGaps.get(dataGaps.size() - 1).getEndId();
                } else {
                    args[i * 2 + 2] = gap.getEndId();
                }
                types[i * 2 + 2] = dataIdSqlType;
            }
        }

        if (!isOracleNoOrder) {
            this.currentGap = dataGaps.remove(0);
        }

        ISqlRowMapper<Data> dataMapper = new ISqlRowMapper<Data>() {
            public Data mapRow(Row row) {
                return engine.getDataService().mapData(row);
            }
        };

        ISqlReadCursor<Data> cursor = null;
        try {
            cursor = sqlTemplate.queryForCursor(sql, dataMapper, args, types);
        } catch (RuntimeException e) {
            log.info("Failed to execute query, but will try again,", e);
            AppUtils.sleep(1000);
            cursor = sqlTemplate.queryForCursor(sql, dataMapper, args, types);
        }
        
        if (isSortInMemory) {
            Comparator<Data> comparator = null;
            if (isOracleNoOrder) {
                comparator = DataMemoryCursor.SORT_BY_TIME;
            } else if (parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, true)) {
                comparator = DataMemoryCursor.SORT_BY_ID;
            }

            cursor = new DataMemoryCursor(cursor, context, comparator);
        }

        return cursor;
    }

    protected String qualifyUsingDataGaps(List<DataGap> dataGaps, int numberOfGapsToQualify,
            String sql) {
        StringBuilder gapClause = new StringBuilder();
        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
            if (i == 0) {
                gapClause.append(" and (");
            } else {
                gapClause.append(" or ");
            }
            gapClause.append("(d.data_id between ? and ?)");
        }
        gapClause.append(")");
        return FormatUtils.replace("dataRange", gapClause.toString(), sql);
    }

    protected String getSql(String sqlName, Channel channel) {
        String select = engine.getRouterService().getSql(sqlName);
        if (!channel.isUseOldDataToRoute() || context.isOnlyDefaultRoutersAssigned()) {
            select = select.replace("d.old_data", "''");
        }
        if (!channel.isUseRowDataToRoute() || context.isOnlyDefaultRoutersAssigned()) {
            select = select.replace("d.row_data", "''");
        }
        if (!channel.isUsePkDataToRoute() || context.isOnlyDefaultRoutersAssigned()) {
            select = select.replace("d.pk_data", "''");
        }
        return engine.getSymmetricDialect().massageDataExtractionSql(
                select, channel);
    }

    protected boolean fillPeekAheadQueue(List<Data> peekAheadQueue, int peekAheadCount,
            ISqlReadCursor<Data> cursor) throws SQLException {
        boolean moreData = true;
        int dataCount = 0;
        long ts = System.currentTimeMillis();
        Data data = null;
        boolean isFirstRead = context.getStartDataId() == 0;
        while (reading && dataCount < peekAheadCount) {
            data = cursor.next();
            if (data != null) {
                if (process(data)) {
                    peekAheadQueue.add(data);
                    peekAheadSizeInBytes += data.getSizeInBytes();                    
                    dataCount++;
                    context.incrementStat(System.currentTimeMillis() - ts,
                            ChannelRouterContext.STAT_READ_DATA_MS);
                } else {
                    context.incrementDataRereadCount();
                    context.incrementStat(System.currentTimeMillis() - ts,
                            ChannelRouterContext.STAT_REREAD_DATA_MS);
                }
                if (isFirstRead) {
                    context.setStartDataId(data.getDataId());
                    isFirstRead = false;
                }
                context.setEndDataId(data.getDataId());
                ts = System.currentTimeMillis();
                
                long totalTimeInMs = System.currentTimeMillis() - lastStatsPrintOutBaselineInMs;
                if (totalTimeInMs > LOG_PROCESS_SUMMARY_THRESHOLD) {
                    log.info(
                            "Reading data to route for channel '{}' has been processing for {} seconds. The following stats have been gathered: datdaRereadCount={}, dataCount={}",
                            context.getChannel().getChannelId(), (System.currentTimeMillis() - context.getCreatedTimeInMs()) / 1000,
                            context.getDataRereadCount(), dataCount + context.getDataReadCount());
                    lastStatsPrintOutBaselineInMs = System.currentTimeMillis();
                }
            } else {
                moreData = false;
                break;
            }

        }
        context.incrementDataReadCount(dataCount);
        context.incrementPeekAheadFillCount(1);
        int size = peekAheadQueue.size();
        if (context.getMaxPeekAheadQueueSize() < size) {
            context.setMaxPeekAheadQueueSize(size);
        }
        return moreData && reading;
    }

    protected void copyToQueue(Data data) {
        long ts = System.currentTimeMillis();
        peekAheadSizeInBytes -= data.getSizeInBytes();
        while (!dataQueue.offer(data) && reading) {
            AppUtils.sleep(50);
        }
        context.incrementStat(System.currentTimeMillis() - ts,
                ChannelRouterContext.STAT_ENQUEUE_DATA_MS);
    }

    public boolean isReading() {
        return reading;
    }

    public void setReading(boolean reading) {
        this.reading = reading;
        if (processInfo.getStatus() != ProcessStatus.ERROR) {
            processInfo.setStatus(ProcessStatus.OK);
        }
    }

    public BlockingQueue<Data> getDataQueue() {
        return dataQueue;
    }

    static class EOD extends Data {
        private static final long serialVersionUID = 1L;
    }

}
