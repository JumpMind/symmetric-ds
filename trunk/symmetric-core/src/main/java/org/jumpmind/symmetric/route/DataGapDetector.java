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

import java.util.Date;
import java.util.List;

import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for managing gaps in data ids to ensure that all captured data is
 * routed for delivery to other nodes.
 */
public class DataGapDetector {

    private static final Logger log = LoggerFactory.getLogger(DataGapDetector.class);

    protected IDataService dataService;

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected IRouterService routerService;
    
    protected IStatisticManager statisticManager;
    
    protected INodeService nodeService;

    public DataGapDetector() {
    }

    public DataGapDetector(IDataService dataService, IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IRouterService routerService, IStatisticManager statisticManager, INodeService nodeService) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.routerService = routerService;
        this.symmetricDialect = symmetricDialect;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
    }
    
    /**
     * Always make sure sym_data_gap is up to date to make sure that we don't
     * dual route data.
     */
    public void beforeRouting() {
        long printStats = System.currentTimeMillis();
        ProcessInfo processInfo = this.statisticManager.newProcessInfo(new ProcessInfoKey(
                nodeService.findIdentityNodeId(), null, ProcessType.GAP_DETECT));
        try {
            long ts = System.currentTimeMillis();
            processInfo.setStatus(Status.QUERYING);
            final List<DataGap> gaps = dataService.findDataGaps();
            long lastDataId = -1;
            final int dataIdIncrementBy = parameterService
                    .getInt(ParameterConstants.DATA_ID_INCREMENT_BY);
            final long maxDataToSelect = parameterService
                    .getInt(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
            long databaseTime = symmetricDialect.getDatabaseTime();
            int idsFilled = 0;
            int newGapsInserted = 0;
            int rangeChecked = 0;
            int gapsDeleted = 0;
            for (final DataGap dataGap : gaps) {
                final boolean lastGap = dataGap.equals(gaps.get(gaps.size() - 1));
                String sql = routerService.getSql("selectDistinctDataIdFromDataEventUsingGapsSql");
                ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
                Object[] params = new Object[] { dataGap.getStartId(), dataGap.getEndId() };
                lastDataId = -1;
                processInfo.setStatus(Status.QUERYING);
                List<Number> ids = sqlTemplate.query(sql, new NumberMapper(), params);
                processInfo.setStatus(Status.PROCESSING);
                
                idsFilled += ids.size();
                rangeChecked += dataGap.getEndId() - dataGap.getStartId();
                
                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();
                    for (Number number : ids) {
                        long dataId = number.longValue();
                        processInfo.incrementCurrentDataCount();
                        if (lastDataId == -1 && dataGap.getStartId() + dataIdIncrementBy <= dataId) {
                            // there was a new gap at the start
                            dataService.insertDataGap(transaction, new DataGap(dataGap.getStartId(), dataId - 1));
                            newGapsInserted++;
                        } else if (lastDataId != -1 && lastDataId + dataIdIncrementBy != dataId && lastDataId != dataId) {
                            // found a gap somewhere in the existing gap
                            dataService.insertDataGap(transaction, new DataGap(lastDataId + 1, dataId - 1));
                            newGapsInserted++;
                        }
                        lastDataId = dataId;
                    }

                    // if we found data in the gap
                    if (lastDataId != -1) {
                        if (!lastGap && lastDataId + dataIdIncrementBy <= dataGap.getEndId()) {
                            dataService.insertDataGap(transaction, new DataGap(lastDataId + dataIdIncrementBy, dataGap.getEndId()));
                            newGapsInserted++;
                        }

                        dataService.deleteDataGap(transaction, dataGap);
                        gapsDeleted++;

                        // if we did not find data in the gap and it was not the
                        // last gap
                    } else if (!lastGap) {
                        if (dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0) {
                            if (symmetricDialect.supportsTransactionViews()) {
                                long transactionViewClockSyncThresholdInMs = parameterService.getLong(
                                        ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS, 60000);
                                Date createTime = dataService.findCreateTimeOfData(dataGap.getEndId() + 1);
                                if (createTime != null
                                        && !symmetricDialect.areDatabaseTransactionsPendingSince(createTime.getTime()
                                                + transactionViewClockSyncThresholdInMs)) {
                                    if (dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0) {
                                        if (dataGap.getStartId() == dataGap.getEndId()) {
                                            log.info(
                                                    "Found a gap in data_id at {}.  Skipping it because there are no pending transactions in the database",
                                                    dataGap.getStartId());
                                        } else {
                                            log.info(
                                                    "Found a gap in data_id from {} to {}.  Skipping it because there are no pending transactions in the database",
                                                    dataGap.getStartId(), dataGap.getEndId());
                                        }

                                        dataService.deleteDataGap(transaction, dataGap);
                                        gapsDeleted++;
                                    }
                                }
                            } else if (isDataGapExpired(dataGap.getEndId() + 1, databaseTime)) {
                                if (dataGap.getStartId() == dataGap.getEndId()) {
                                    log.info("Found a gap in data_id at {}.  Skipping it because the gap expired", dataGap.getStartId());
                                } else {
                                    log.info("Found a gap in data_id from {} to {}.  Skipping it because the gap expired",
                                            dataGap.getStartId(), dataGap.getEndId());
                                }
                                dataService.deleteDataGap(transaction, dataGap);
                                gapsDeleted++;
                            }
                        } 
                    }

                    if (System.currentTimeMillis() - printStats > 30000) {
                        log.info(
                                "The data gap detection process has been running for {}ms, detected {} rows that have been previously routed over a total gap range of {}, "
                                        + "inserted {} new gaps, and deleted {} gaps", new Object[] { System.currentTimeMillis() - ts,
                                        idsFilled, rangeChecked, newGapsInserted, gapsDeleted });
                        printStats = System.currentTimeMillis();
                    }

                    transaction.commit();
                } catch (Error ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } catch (RuntimeException ex) {
                    if (transaction != null) {
                        transaction.rollback();
                    }
                    throw ex;
                } finally {
                    if (transaction != null) {
                        transaction.close();
                    }
                }
            }

            if (lastDataId != -1) {
                dataService
                        .insertDataGap(new DataGap(lastDataId + 1, lastDataId + maxDataToSelect));
            }

            long updateTimeInMs = System.currentTimeMillis() - ts;
            if (updateTimeInMs > 10000) {
                log.info("Detecting gaps took {} ms", updateTimeInMs);
            }
            processInfo.setStatus(Status.OK);
        } catch (RuntimeException ex) {
            processInfo.setStatus(Status.ERROR);
            throw ex;
        }

    }

    protected boolean isDataGapExpired(long dataId, long databaseTime) {
        long gapTimoutInMs = parameterService
                .getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        Date createTime = dataService.findCreateTimeOfData(dataId);
        if (createTime == null) {
            createTime = dataService.findNextCreateTimeOfDataStartingAt(dataId);
        }        
        if (createTime != null && databaseTime - createTime.getTime() > gapTimoutInMs) {
            return true;
        } else {
            return false;
        }
    }

}