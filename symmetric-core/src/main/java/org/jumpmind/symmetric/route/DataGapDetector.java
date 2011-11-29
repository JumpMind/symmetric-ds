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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * Responsible for managing gaps in data ids to ensure that all captured data is
 * routed for delivery to other nodes.
 */
public class DataGapDetector implements IDataToRouteGapDetector {

    final ILog log = LogFactory.getLog(getClass());

    private IDataService dataService;

    private IParameterService parameterService;

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private ISqlProvider sqlProvider;

    public DataGapDetector() {
    }

    public DataGapDetector(IDataService dataService, IParameterService parameterService,
            JdbcTemplate jdbcTemplate, IDbDialect dbDialect, ISqlProvider sqlProvider) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.sqlProvider = sqlProvider;
    }

    public void afterRouting() {
    }

    /**
     * Always make sure sym_data_gap is up to date to make sure that we don't
     * dual route data.
     */
    public void beforeRouting() {
        long ts = System.currentTimeMillis();
        final List<DataGap> gaps = removeAbandonedGaps(dataService.findDataGaps());
        long lastDataId = -1;
        final int dataIdIncrementBy = parameterService
                .getInt(ParameterConstants.DATA_ID_INCREMENT_BY);
        final long maxDataToSelect = parameterService
                .getInt(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
        for (final DataGap dataGap : gaps) {
            final boolean lastGap = dataGap.equals(gaps.get(gaps.size() - 1));
            String sql = sqlProvider.getSql("selectDistinctDataIdFromDataEventUsingGapsSql");
            Object[] params = new Object[] { dataGap.getStartId(), dataGap.getEndId() };
            lastDataId = jdbcTemplate.query(sql, params, new ResultSetExtractor<Long>() {
                public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
                    long lastDataId = -1;
                    while (rs.next()) {
                        long dataId = rs.getLong(1);
                        if (lastDataId == -1 && dataGap.getStartId() < dataId) {
                            // there was a new gap at the start
                            dataService.insertDataGap(new DataGap(dataGap.getStartId(), dataId - 1));
                        } else if (lastDataId != -1 && lastDataId + dataIdIncrementBy != dataId
                                && lastDataId != dataId) {
                            // found a gap somewhere in the existing gap
                            dataService.insertDataGap(new DataGap(lastDataId + 1, dataId - 1));
                        }
                        lastDataId = dataId;
                    }

                    // if we found data in the gap
                    if (lastDataId != -1) {
                        if (!lastGap && lastDataId < dataGap.getEndId()) {
                            dataService.insertDataGap(new DataGap(lastDataId + 1, dataGap
                                    .getEndId()));
                        }
                        dataService.updateDataGap(dataGap, DataGap.Status.OK);

                        // if we did not find data in the gap and it was not the
                        // last gap
                    } else if (!lastGap) {
                        if (dataService.countDataInRange(dataGap.getStartId() - 1,
                                dataGap.getEndId() + 1) == 0) {
                            if (dbDialect.supportsTransactionViews()) {
                                long transactionViewClockSyncThresholdInMs = parameterService
                                        .getLong(
                                                ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS,
                                                60000);
                                Date createTime = dataService.findCreateTimeOfData(dataGap
                                        .getEndId() + 1);
                                if (createTime != null
                                        && !dbDialect
                                                .areDatabaseTransactionsPendingSince(createTime
                                                        .getTime()
                                                        + transactionViewClockSyncThresholdInMs)) {
                                    if (dataService.countDataInRange(dataGap.getStartId() - 1,
                                            dataGap.getEndId() + 1) == 0) {
                                        log.info("RouterSkippingDataIdsNoTransactions",
                                                dataGap.getStartId(), dataGap.getEndId());
                                        dataService.updateDataGap(dataGap, DataGap.Status.SK);
                                    }
                                }
                            } else if (isDataGapExpired(dataGap.getEndId() + 1)) {
                                log.info("RouterSkippingDataIdsGapExpired", dataGap.getStartId(),
                                        dataGap.getEndId());
                                dataService.updateDataGap(dataGap, DataGap.Status.SK);
                            }
                        } else {
                            dataService.checkForAndUpdateMissingChannelIds(
                                    dataGap.getStartId() - 1, dataGap.getEndId() + 1);
                        }
                    }

                    return lastDataId;
                }

            });
        }

        if (lastDataId != -1) {
            dataService.insertDataGap(new DataGap(lastDataId + 1, lastDataId + maxDataToSelect));            
        }

        long updateTimeInMs = System.currentTimeMillis() - ts;
        if (updateTimeInMs > 10000) {
            log.info("RoutedGapDetectionTime", updateTimeInMs);
        }

    }

    /**
     * If the system was shutdown in the middle of processing a large gap we
     * could end up with a gap containing other gaps.
     * 
     * @param gaps
     */
    protected List<DataGap> removeAbandonedGaps(List<DataGap> gaps) {
        List<DataGap> finalList = new ArrayList<DataGap>(gaps);
        for (final DataGap dataGap1 : gaps) {
            for (final DataGap dataGap2 : gaps) {
                if (!dataGap1.equals(dataGap2) && dataGap1.contains(dataGap2)) {
                    finalList.remove(dataGap2);
                    if (dataService != null) {
                        dataService.updateDataGap(dataGap2, DataGap.Status.SK);
                    }
                }
            }
        }
        return finalList;
    }

    protected boolean isDataGapExpired(long dataId) {
        long gapTimoutInMs = parameterService
                .getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfData(dataId);
        if (createTime != null && System.currentTimeMillis() - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }

}