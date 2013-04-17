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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * This class is responsible for reading data for the purpose of routing. It
 * reads ahead and tries to keep a blocking queue populated for another thread
 * to process.
 */
public class DataGapRouteReader extends AbstractDataToRouteReader {

    private static final String SELECT_DATA_USING_GAPS_SQL = "selectDataUsingGapsSql";

    protected DataGap currentGap;

    protected List<DataGap> dataGaps;

    public DataGapRouteReader(ILog log, ISqlProvider sqlProvider, ChannelRouterContext context,
            IDataService dataService, JdbcTemplate jdbcTemplate, IDbDialect dbDialect) {
        super(log, sqlProvider, context, dataService, jdbcTemplate, dbDialect);
    }
    
    @Override
    protected void execute() {
        jdbcTemplate.execute(new ConnectionCallback<Integer>() {
            public Integer doInConnection(Connection c) throws SQLException, DataAccessException {
                int dataCount = 0;
                PreparedStatement ps = null;
                ResultSet rs = null;
                boolean autoCommit = c.getAutoCommit();
                try {
                    int numberOfGapsToQualify = 1;
                    dataGaps = dataService.findDataGaps();

                    if (dbDialect.requiresAutoCommitFalseToSetFetchSize()) {
                        c.setAutoCommit(false);
                    }
                    String channelId = context.getChannel().getChannelId();

                    ps = prepareStatment(dataGaps, numberOfGapsToQualify, c);
                    long ts = System.currentTimeMillis();
                    rs = ps.executeQuery();
                    long executeTimeInMs = System.currentTimeMillis() - ts;
                    context.incrementStat(executeTimeInMs, ChannelRouterContext.STAT_QUERY_TIME_MS);
                    if (executeTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
                        log.warn("RoutedDataSelectedInTime", executeTimeInMs, channelId);
                    } else if (log.isDebugEnabled()) {
                        log.debug("RoutedDataSelectedInTime", executeTimeInMs, channelId);
                    }

                    long maxDataToRoute = context.getChannel().getMaxDataToRoute();
                    String lastTransactionId = null;
                    int peekAheadCount = dbDialect.getRouterDataPeekAheadCount();
                    List<Data> peekAheadQueue = new ArrayList<Data>(peekAheadCount);
                    boolean nontransactional = context.getChannel().getBatchAlgorithm()
                            .equals("nontransactional");

                    boolean moreData = true;
                    while (dataCount <= maxDataToRoute || lastTransactionId != null) {
                        if (moreData) {
                            moreData = fillPeekAheadQueue(peekAheadQueue, peekAheadCount, rs);
                        }

                        if ((lastTransactionId == null || nontransactional)
                                && peekAheadQueue.size() > 0) {
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
                    return dataCount;

                } finally {
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(ps);
                    rs = null;
                    ps = null;

                    if (dbDialect.requiresAutoCommitFalseToSetFetchSize()) {
                        c.commit();
                        c.setAutoCommit(autoCommit);
                    }

                    boolean done = false;
                    do {
                        done = dataQueue.offer(new EOD());
                        if (!done) {
                            AppUtils.sleep(50);
                        }
                    } while (!done && reading);

                    reading = false;

                }
            }
        });
    }
    

    protected PreparedStatement prepareStatment(List<DataGap> dataGaps, int numberOfGapsToQualify,
            Connection c) throws SQLException {
        String channelId = context.getChannel().getChannelId();
        String sql = qualifyUsingDataGaps(dataGaps, numberOfGapsToQualify,
                getSql(SELECT_DATA_USING_GAPS_SQL, context.getChannel().getChannel()));
        PreparedStatement ps = c.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
        ps.setFetchSize(jdbcTemplate.getFetchSize());
        ps.setString(1, channelId);
        for (int i = 0; i < numberOfGapsToQualify && i < dataGaps.size(); i++) {
            DataGap gap = dataGaps.get(i);
            ps.setLong(i * 2 + 2, gap.getStartId());
            if ((i + 1) == numberOfGapsToQualify && (i + 1) < dataGaps.size()) {
                // there were more gaps than we are going to use in the SQL. use
                // the last gap as the end data id for the last range
                ps.setLong(i * 2 + 3, dataGaps.get(dataGaps.size() - 1).getEndId());
            } else {
                ps.setLong(i * 2 + 3, gap.getEndId());
            }
        }
        
        if (dataGaps.size() > 0) {
            currentGap = dataGaps.remove(0);
        }
        return ps;
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
        return AppUtils.replace("dataRange", gapClause.toString(), sql);
    }


    protected boolean process(ResultSet rs) throws SQLException {
        long dataId = rs.getLong(1);
        if (currentGap != null && dataId >= currentGap.getStartId()) {
            if (dataId <= currentGap.getEndId()) {
                return true;
            } else {
                // past current gap. move to next gap
                if (dataGaps.size() > 0) {
                    currentGap = dataGaps.remove(0);
                    return process(rs);
                } else {
                    currentGap = null;
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    protected void copyToQueue(Data data) {
        long ts = System.currentTimeMillis();
        while (!dataQueue.offer(data) && reading) {
            AppUtils.sleep(50);
        }
        context.incrementStat(System.currentTimeMillis() - ts,
                ChannelRouterContext.STAT_ENQUEUE_DATA_MS);
    }

    protected boolean fillPeekAheadQueue(List<Data> peekAheadQueue, int peekAheadCount, ResultSet rs)
            throws SQLException {
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

}