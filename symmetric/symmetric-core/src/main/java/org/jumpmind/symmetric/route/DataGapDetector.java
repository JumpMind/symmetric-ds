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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for managing gaps in data ids to ensure that all captured data is
 * routed for delivery to other nodes.
 */
public class DataGapDetector implements IDataToRouteGapDetector {

    private static final Logger log = LoggerFactory.getLogger(DataGapDetector.class);

    private IDataService dataService;

    private IParameterService parameterService;

    private ISymmetricDialect symmetricDialect;

    private IRouterService routerService;

    public DataGapDetector() {
    }

    public DataGapDetector(IDataService dataService, IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IRouterService routerService) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.routerService = routerService;
        this.symmetricDialect = symmetricDialect;
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
            String sql = routerService.getSql("selectDistinctDataIdFromDataEventUsingGapsSql");
            ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
            Object[] params = new Object[] { dataGap.getStartId(), dataGap.getEndId() };
            lastDataId = -1;
            List<Number> ids = sqlTemplate.query(sql, new NumberMapper(), params);
            for (Number number : ids) {
                long dataId = number.longValue();
                if (lastDataId == -1 && dataGap.getStartId() + dataIdIncrementBy <= dataId) {
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
                if (!lastGap && lastDataId + dataIdIncrementBy <= dataGap.getEndId()) {
                    dataService.insertDataGap(new DataGap(lastDataId + dataIdIncrementBy, dataGap.getEndId()));
                }
                dataService.updateDataGap(dataGap, DataGap.Status.OK);

                // if we did not find data in the gap and it was not the
                // last gap
            } else if (!lastGap) {
                if (dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0) {
                    if (symmetricDialect.supportsTransactionViews()) {
                        long transactionViewClockSyncThresholdInMs = parameterService
                                .getLong(
                                        ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS,
                                        60000);
                        Date createTime = dataService.findCreateTimeOfData(dataGap.getEndId() + 1);
                        if (createTime != null
                                && !symmetricDialect.areDatabaseTransactionsPendingSince(createTime
                                        .getTime() + transactionViewClockSyncThresholdInMs)) {
                            if (dataService.countDataInRange(dataGap.getStartId() - 1,
                                    dataGap.getEndId() + 1) == 0) {
                                if (dataGap.getStartId() == dataGap.getEndId()) {
                                    log.info(
                                            "Found a gap in data_id at {}.  Skipping it because there are no pending transactions in the database",
                                            dataGap.getStartId());
                                } else {
                                    log.info(
                                            "Found a gap in data_id from {} to {}.  Skipping it because there are no pending transactions in the database",
                                            dataGap.getStartId(), dataGap.getEndId());
                                }
                                dataService.updateDataGap(dataGap, DataGap.Status.SK);
                            }
                        }
                    } else if (isDataGapExpired(dataGap.getEndId() + 1)) {
                        if (dataGap.getStartId() == dataGap.getEndId()) {
                            log.info(
                                    "Found a gap in data_id at {}.  Skipping it because the gap expired",
                                    dataGap.getStartId());
                        } else {
                            log.info(
                                    "Found a gap in data_id from {} to {}.  Skipping it because the gap expired",
                                    dataGap.getStartId(), dataGap.getEndId());
                        }
                        dataService.updateDataGap(dataGap, DataGap.Status.SK);
                    }
                } else {
                    dataService.checkForAndUpdateMissingChannelIds(dataGap.getStartId() - 1,
                            dataGap.getEndId() + 1);
                }
            }
        }

        if (lastDataId != -1) {
            dataService.insertDataGap(new DataGap(lastDataId + 1, lastDataId + maxDataToSelect));
        }

        long updateTimeInMs = System.currentTimeMillis() - ts;
        if (updateTimeInMs > 10000) {
            log.info("Detecting gaps took {} ms", updateTimeInMs);
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
