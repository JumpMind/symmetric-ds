/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.hsqldb.types.Types;
import org.jumpmind.db.sql.ISqlMap;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.util.FormatUtils;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for another thread
 * to process.
 */
public class DataGapRouteReader implements IDataToRouteReader {

    private static final String SELECT_DATA_USING_GAPS_SQL = "selectDataUsingGapsSql";

    protected List<DataGap> dataGaps;

    protected DataGap currentGap;

    protected Log log;

    protected BlockingQueue<Data> dataQueue;

    protected ChannelRouterContext context;

    protected IDataService dataService;

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected ISqlMap sqlMap;

    protected boolean reading = true;

    public DataGapRouteReader(Log log, ISqlMap sqlMap, ChannelRouterContext context,
            IDataService dataService, ISymmetricDialect symmetricDialect,
            IParameterService parameterService) {
        this.log = log;
        this.sqlMap = sqlMap;
        this.symmetricDialect = symmetricDialect;
        this.dataQueue = new LinkedBlockingQueue<Data>(
                symmetricDialect != null ? symmetricDialect.getRouterDataPeekAheadCount() : 1000);
        this.context = context;
        this.dataService = dataService;
        this.parameterService = parameterService;
    }

    public void run() {
        try {
            execute();
        } catch (Throwable ex) {
            log.error(ex);
        }
    }

    protected void execute() {

        ISqlReadCursor<Data> cursor = null;
        try {
            int dataCount = 0;

            long maxDataToRoute = context.getChannel().getMaxDataToRoute();
            int peekAheadCount = symmetricDialect.getRouterDataPeekAheadCount();
            String lastTransactionId = null;
            List<Data> peekAheadQueue = new ArrayList<Data>(peekAheadCount);
            boolean nontransactional = context.getChannel().getBatchAlgorithm()
                    .equals("nontransactional");

            cursor = prepareCursor();

            boolean moreData = true;
            while (dataCount <= maxDataToRoute || lastTransactionId != null) {
                if (moreData) {
                    moreData = fillPeekAheadQueue(peekAheadQueue, peekAheadCount, cursor);
                }

                if ((lastTransactionId == null || nontransactional) && peekAheadQueue.size() > 0) {
                    Data data = peekAheadQueue.remove(0);
                    copyToQueue(data);
                    dataCount++;
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

        } catch (Throwable ex) {
            log.error(ex);
        } finally {
            copyToQueue(new EOD());
            reading = false;
            if (cursor != null) {
                cursor.close();
            }

        }

    }

    protected boolean process(Data data) {
        long dataId = data.getDataId();
        if (currentGap != null && dataId >= currentGap.getStartId()) {
            if (dataId <= currentGap.getEndId()) {
                return true;
            } else {
                // past current gap. move to next gap
                if (dataGaps.size() > 0) {
                    currentGap = dataGaps.remove(0);
                    return process(data);
                } else {
                    currentGap = null;
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    public Data take() {
        Data data = null;
        try {
            int timeout = parameterService.getInt(
                    ParameterConstants.ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS, 330);
            data = dataQueue.poll(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn(e);
        }

        if (data == null) {
            throw new SymmetricException("RouterDataReaderNotResponding");
        } else if (data instanceof EOD) {
            data = null;
        }
        return data;
    }

    protected ISqlReadCursor<Data> prepareCursor() {
        int numberOfGapsToQualify = parameterService.getInt(
                ParameterConstants.ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL, 100);

        this.dataGaps = dataService.findDataGaps();

        String channelId = context.getChannel().getChannelId();

        String sql = qualifyUsingDataGaps(dataGaps, numberOfGapsToQualify,
                getSql(SELECT_DATA_USING_GAPS_SQL, context.getChannel().getChannel()));

        ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();

        int numberOfArgs = 1 + (numberOfGapsToQualify < dataGaps.size() ? numberOfGapsToQualify
                : dataGaps.size());
        Object[] args = new Object[numberOfArgs];
        int[] types = new int[numberOfArgs];
        args[0] = channelId;
        types[0] = Types.VARCHAR;

        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
            DataGap gap = dataGaps.get(i);
            args[i * 2 + 1] = gap.getStartId();
            types[i * 2 + 1] = Types.NUMERIC;
            if ((i + 1) == numberOfGapsToQualify && (i + 1) < dataGaps.size()) {
                // there were more gaps than we are going to use in the SQL. use
                // the last gap as the end data id for the last range
                args[i * 2 + 2] = dataGaps.get(dataGaps.size() - 1).getEndId();
            } else {
                args[i * 2 + 2] = gap.getEndId();
            }
            types[i * 2 + 2] = Types.NUMERIC;
        }

        this.currentGap = dataGaps.remove(0);

        return sqlTemplate.queryForCursor(sql, new ISqlRowMapper<Data>() {
            public Data mapRow(Row row) {
                Data data = new Data();
                data.putCsvData(CsvData.ROW_DATA, row.getString("ROW_DATA"));
                data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA"));
                data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA"));
                data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID"));
                data.setDataEventType(DataEventType.getEventType(row.getString("EVENT_TYPE")));
                data.putAttribute(CsvData.ATTRIBUTE_TABLE_ID, row.getInt("TRIGGER_HIST_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID, row.getString("SOURCE_NODE_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_ROUTER_ID, row.getString("ROUTER_ID"));
                data.putAttribute(CsvData.ATTRIBUTE_EXTERNAL_DATA, row.getString("EXTERNAL_DATA"));
                data.putAttribute(CsvData.ATTRIBUTE_DATA_ID, row.getLong("DATA_ID"));
                return data;
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
        String select = sqlMap.getSql(sqlName);
        if (!channel.isUseOldDataToRoute()) {
            select = select.replace("d.old_data", "''");
        }
        if (!channel.isUseRowDataToRoute()) {
            select = select.replace("d.row_data", "''");
        }
        if (!channel.isUsePkDataToRoute()) {
            select = select.replace("d.pk_data", "''");
        }
        return symmetricDialect == null ? select : symmetricDialect.massageDataExtractionSql(
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
                    context.setLastDataIdForTransactionId(data);
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

    protected ResultSet executeQuery(PreparedStatement ps) throws SQLException {
        long ts = System.currentTimeMillis();
        ResultSet rs = ps.executeQuery();
        long executeTimeInMs = System.currentTimeMillis() - ts;
        context.incrementStat(executeTimeInMs, ChannelRouterContext.STAT_QUERY_TIME_MS);
        if (executeTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
            log.warn("RoutedDataSelectedInTime", executeTimeInMs, context.getChannel()
                    .getChannelId());
        } else if (log.isDebugEnabled()) {
            log.debug("RoutedDataSelectedInTime", executeTimeInMs, context.getChannel()
                    .getChannelId());
        }
        return rs;
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