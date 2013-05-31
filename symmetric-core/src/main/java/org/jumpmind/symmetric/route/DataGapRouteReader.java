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

import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
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

    protected boolean reading = true;
    
    protected int peekAheadCount = 1000;
    
    protected int takeTimeout;
    
    protected static Map<String, Boolean> lastSelectUsedGreaterThanQueryByEngineName = new HashMap<String, Boolean>(); 

    public DataGapRouteReader(ChannelRouterContext context, ISymmetricEngine engine) {
        this.engine = engine;
        IParameterService parameterService = engine.getParameterService();
        this.peekAheadCount = parameterService.getInt(ParameterConstants.ROUTING_PEEK_AHEAD_WINDOW);
        this.takeTimeout = engine.getParameterService().getInt(
                ParameterConstants.ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS, 330);
        this.dataQueue = new LinkedBlockingQueue<Data>(peekAheadCount);
        this.context = context;
        
        String engineName = parameterService.getEngineName();
        if (lastSelectUsedGreaterThanQueryByEngineName.get(engineName) == null) {
            lastSelectUsedGreaterThanQueryByEngineName.put(engineName, Boolean.FALSE);
        }
    }

    public void run() {
        try {
            execute();
        } catch (Throwable ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    protected void execute() {
        ISymmetricDialect symmetricDialect = engine.getSymmetricDialect();
        ISqlReadCursor<Data> cursor = null;
        ProcessInfo processInfo = engine.getStatisticManager().
                newProcessInfo(new ProcessInfoKey(engine.getNodeService().findIdentityNodeId(), null, ProcessType.ROUTER_READER));
        processInfo.setCurrentChannelId(context.getChannel().getChannelId());
        try {
            int dataCount = 0;

            long maxDataToRoute = context.getChannel().getMaxDataToRoute();
            String lastTransactionId = null;
            List<Data> peekAheadQueue = new ArrayList<Data>(peekAheadCount);
            boolean nontransactional = context.getChannel().getBatchAlgorithm()
                    .equals("nontransactional")
                    || !symmetricDialect.supportsTransactionId();

            processInfo.setStatus(Status.QUERYING);
            cursor = prepareCursor();
            processInfo.setStatus(Status.EXTRACTING);
            boolean moreData = true;
            while (dataCount <= maxDataToRoute || lastTransactionId != null) {
                if (moreData) {
                    moreData = fillPeekAheadQueue(peekAheadQueue, peekAheadCount, cursor);
                }

                if ((lastTransactionId == null || nontransactional) && peekAheadQueue.size() > 0) {
                    Data data = peekAheadQueue.remove(0);
                    copyToQueue(data);
                    dataCount++;
                    processInfo.incrementDataCount();
                    processInfo.setCurrentTableName(data.getTableName());
                    lastTransactionId = data.getTransactionId();
                } else if (lastTransactionId != null && peekAheadQueue.size() > 0) {
                    Iterator<Data> datas = peekAheadQueue.iterator();
                    int dataWithSameTransactionIdCount = 0;
                    while (datas.hasNext()) {
                        Data data = datas.next();
                        if (lastTransactionId.equals(data.getTransactionId())) {
                            dataWithSameTransactionIdCount++;
                            datas.remove();
                            copyToQueue(data);
                            dataCount++;                            
                            processInfo.incrementDataCount();
                            processInfo.setCurrentTableName(data.getTableName());
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
            processInfo.setStatus(Status.DONE);
        } catch (Throwable ex) {
            processInfo.setStatus(Status.ERROR);
            log.error(ex.getMessage(), ex);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            copyToQueue(new EOD());
            reading = false;
        }

    }

    protected boolean process(Data data) {
        long dataId = data.getDataId();
        boolean okToProcess = false;
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
        return okToProcess;
    }

    public Data take() throws InterruptedException {
        Data data = dataQueue.poll(takeTimeout, TimeUnit.SECONDS);

        if (data == null) {
            throw new SymmetricException("The read of the data to route queue has timed out");
        } else if (data instanceof EOD) {
            data = null;
        }
        return data;
    }

    protected ISqlReadCursor<Data> prepareCursor() {
        IParameterService parameterService = engine.getParameterService();
        int numberOfGapsToQualify = parameterService.getInt(
                ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);
        
        int maxGapsBeforeGreaterThanQuery = parameterService.getInt(ParameterConstants.ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY, 100);

        this.dataGaps = engine.getDataService().findDataGaps();
                
        boolean useGreaterThanDataId = false;
        if (maxGapsBeforeGreaterThanQuery > 0 && this.dataGaps.size() > maxGapsBeforeGreaterThanQuery) {
            useGreaterThanDataId = true;
        }

        String channelId = context.getChannel().getChannelId();

        String sql = null;
        
        Boolean lastSelectUsedGreaterThanQuery = lastSelectUsedGreaterThanQueryByEngineName.get(parameterService.getEngineName());
        if (lastSelectUsedGreaterThanQuery == null) {
            lastSelectUsedGreaterThanQuery = Boolean.FALSE;
        }
        
        if (useGreaterThanDataId) {            
            sql = getSql("selectDataUsingStartDataId", context.getChannel().getChannel());
            if (!lastSelectUsedGreaterThanQuery) {
                log.info("Switching to select from the data table where data_id >= start gap");
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
        
        if (parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, true)) {
            sql = sql + engine.getRouterService().getSql("orderByDataId");
        }

        ISqlTemplate sqlTemplate = engine.getSymmetricDialect().getPlatform().getSqlTemplate();
        Object[] args = null;
        int[] types = null;
        
        if (useGreaterThanDataId) {
            args = new Object[] { channelId, dataGaps.get(0).getStartId() };
            types = new int[] { Types.VARCHAR, Types.NUMERIC };
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
                types[i * 2 + 1] = Types.NUMERIC;
                if ((i + 1) == numberOfGapsToQualify && (i + 1) < dataGaps.size()) {
                    // there were more gaps than we are going to use in the SQL.
                    // use
                    // the last gap as the end data id for the last range
                    args[i * 2 + 2] = dataGaps.get(dataGaps.size() - 1).getEndId();
                } else {
                    args[i * 2 + 2] = gap.getEndId();
                }
                types[i * 2 + 2] = Types.NUMERIC;
            }
        }

        this.currentGap = dataGaps.remove(0);

        return sqlTemplate.queryForCursor(sql, new ISqlRowMapper<Data>() {
            public Data mapRow(Row row) {
                return engine.getDataService().mapData(row);
            }
        }, args, types);

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
        if (!channel.isUseOldDataToRoute()) {
            select = select.replace("d.old_data", "''");
        }
        if (!channel.isUseRowDataToRoute()) {
            select = select.replace("d.row_data", "''");
        }
        if (!channel.isUsePkDataToRoute()) {
            select = select.replace("d.pk_data", "''");
        }
        return engine.getSymmetricDialect().massageDataExtractionSql(
                select, channel);
    }

    protected boolean fillPeekAheadQueue(List<Data> peekAheadQueue, int peekAheadCount,
            ISqlReadCursor<Data> cursor) throws SQLException {
        boolean moreData = true;
        int toRead = peekAheadCount - peekAheadQueue.size();
        int dataCount = 0;
        long ts = System.currentTimeMillis();
        while (reading && dataCount < toRead) {
            Data data = cursor.next();
            if (data != null) {
                if (process(data)) {
                    peekAheadQueue.add(data);
                    dataCount++;
                    context.incrementStat(System.currentTimeMillis() - ts,
                            ChannelRouterContext.STAT_READ_DATA_MS);
                } else {
                    context.incrementStat(System.currentTimeMillis() - ts,
                            ChannelRouterContext.STAT_REREAD_DATA_MS);
                }

                ts = System.currentTimeMillis();
            } else {
                moreData = false;
                break;
            }

        }
        return moreData;
    }

    protected void copyToQueue(Data data) {
        long ts = System.currentTimeMillis();
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
    }

    public BlockingQueue<Data> getDataQueue() {
        return dataQueue;
    }

    class EOD extends Data {
        private static final long serialVersionUID = 1L;
    }

}
