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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.service.IContextService;
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
public class DataGapFastDetector extends DataGapDetector implements ISqlRowMapper<Long> {

    private static final Logger log = LoggerFactory.getLogger(DataGapDetector.class);

    protected IDataService dataService;

    protected IParameterService parameterService;
    
    protected IContextService contextService;

    protected ISymmetricDialect symmetricDialect;

    protected IStatisticManager statisticManager;
    
    protected List<DataGap> gaps;
    
    protected List<Long> dataIds;
    
    protected boolean isAllDataRead;

    public DataGapFastDetector(IDataService dataService, IParameterService parameterService, IContextService contextService,
            ISymmetricDialect symmetricDialect, IRouterService routerService, IStatisticManager statisticManager, INodeService nodeService) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.contextService = contextService;
        this.routerService = routerService;
        this.symmetricDialect = symmetricDialect;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
        dataIds = new ArrayList<Long>();
    }

    public void beforeRouting() {
        ProcessInfo processInfo = this.statisticManager.newProcessInfo(new ProcessInfoKey(
                nodeService.findIdentityNodeId(), null, ProcessType.GAP_DETECT));
        processInfo.setStatus(Status.QUERYING);
        gaps = dataService.findDataGaps();
        processInfo.setStatus(Status.OK);

        if (contextService.is(ContextConstants.ROUTING_FULL_GAP_ANALYSIS)) {
            log.info("Full gap analysis is running");
            long ts = System.currentTimeMillis();
            queryDataIdMap();
            log.info("Querying data in gaps from database took {} ms", System.currentTimeMillis() - ts);
            afterRouting();
            log.info("Full gap analysis is done after {} ms", System.currentTimeMillis() - ts);
        }
    }

    /**
     * Always make sure sym_data_gap is up to date to make sure that we don't
     * dual route data.
     */
    public void afterRouting() {
        ProcessInfo processInfo = this.statisticManager.newProcessInfo(new ProcessInfoKey(
                nodeService.findIdentityNodeId(), null, ProcessType.GAP_DETECT));
        processInfo.setStatus(Status.PROCESSING);

        long printStats = System.currentTimeMillis();
        long gapTimoutInMs = parameterService.getLong(ParameterConstants.ROUTING_STALE_DATA_ID_GAP_TIME);
        final int dataIdIncrementBy = parameterService.getInt(ParameterConstants.DATA_ID_INCREMENT_BY);
        final long maxDataToSelect = parameterService.getInt(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);

        long databaseTime = symmetricDialect.getDatabaseTime();
        ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();

        boolean supportsTransactionViews = symmetricDialect.supportsTransactionViews();
        long earliestTransactionTime = 0;
        if (supportsTransactionViews) {
            Date date = symmetricDialect.getEarliestTransactionStartTime();
            if (date != null) {
                earliestTransactionTime = date.getTime() - parameterService.getLong(
                        ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS, 60000);
            }
        }

        try {
            long ts = System.currentTimeMillis();
            long lastDataId = -1;
            int idsFilled = 0;
            int newGapsInserted = 0;
            int rangeChecked = 0;
            int gapsDeleted = 0;
            Set<DataGap> gapCheck = new HashSet<DataGap>(gaps);
            Map<DataGap, List<Long>> dataIdMap = getDataIdMap();

            for (final DataGap dataGap : gaps) {
                final boolean lastGap = dataGap.equals(gaps.get(gaps.size() - 1));
                lastDataId = -1;
                List<Long> ids = dataIdMap.get(dataGap);
                
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
                            DataGap newGap = new DataGap(dataGap.getStartId(), dataId - 1);
                            if (!gapCheck.contains(newGap)) {
                                dataService.insertDataGap(transaction, newGap);
                                gapCheck.add(newGap);
                            }
                            newGapsInserted++;
                        } else if (lastDataId != -1 && lastDataId + dataIdIncrementBy != dataId && lastDataId != dataId) {
                            // found a gap somewhere in the existing gap
                            DataGap newGap = new DataGap(lastDataId + 1, dataId - 1);
                            if (!gapCheck.contains(newGap)) {
                                dataService.insertDataGap(transaction, newGap);
                                gapCheck.add(newGap);
                            }
                            newGapsInserted++;
                        }
                        lastDataId = dataId;
                    }

                    // if we found data in the gap
                    if (lastDataId != -1) {
                        if (!lastGap && lastDataId + dataIdIncrementBy <= dataGap.getEndId()) {
                            DataGap newGap = new DataGap(lastDataId + dataIdIncrementBy, dataGap.getEndId());
                            if (!gapCheck.contains(newGap)) {
                                dataService.insertDataGap(transaction, newGap);
                                gapCheck.add(newGap);
                            }
                            newGapsInserted++;
                        }

                        dataService.deleteDataGap(transaction, dataGap);
                        gapsDeleted++;

                        // if we did not find data in the gap and it was not the
                        // last gap
                    } else if (!lastGap) {
                        Date createTime = dataGap.getCreateTime();
                        if (supportsTransactionViews) {
                            if (createTime != null && (createTime.getTime() < earliestTransactionTime || earliestTransactionTime == 0)) {
                                if (isAllDataRead || dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0) {
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
                        } else if (createTime != null && databaseTime - createTime.getTime() > gapTimoutInMs) {
                            if (isAllDataRead || dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0) {
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
                DataGap newGap = new DataGap(lastDataId + 1, lastDataId + maxDataToSelect);
                if (!gapCheck.contains(newGap)) {
                    dataService.insertDataGap(newGap);
                    gapCheck.add(newGap);
                }
            }

            contextService.save(ContextConstants.ROUTING_FULL_GAP_ANALYSIS, "false");

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

    protected void queryDataIdMap() {
        String sql = routerService.getSql("selectDistinctDataIdFromDataEventUsingGapsSql");
        ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();

        for (DataGap dataGap : gaps) {
            long queryForIdsTs = System.currentTimeMillis();
            Object[] params = new Object[] { dataGap.getStartId(), dataGap.getEndId() };
            List<Long> ids = sqlTemplate.query(sql, this, params);
            dataIds.addAll(ids);
            if (System.currentTimeMillis()-queryForIdsTs > Constants.LONG_OPERATION_THRESHOLD) {
                log.info("It took longer than {}ms to run the following sql for gap from {} to {}.  {}", 
                        new Object[] {Constants.LONG_OPERATION_THRESHOLD, dataGap.getStartId(), dataGap.getEndId(), sql});
            }
        }
    }

    public Long mapRow(Row row) {
        return row.getLong("data_id");
    }

    protected Map<DataGap, List<Long>> getDataIdMap() {
        HashMap<DataGap, List<Long>> map = new HashMap<DataGap, List<Long>>();
        Collections.sort(dataIds);

        for (DataGap gap : gaps) {
            map.put(gap, new ArrayList<Long>());
        }
        
        for (Long dataId : dataIds) {
            long id = dataId.longValue();
            for (DataGap gap : gaps) {
                if (id >= gap.getStartId() && id <= gap.getEndId()) {
                    map.get(gap).add(dataId);
                }
            }
        }

        // we no longer need the list of data IDs, so allow garbage collection 
        dataIds = new ArrayList<Long>();
        return map;
    }
    
    public List<DataGap> getDataGaps() {
        return gaps;
    }
    
    public void addDataIds(List<Long> dataIds) {
        this.dataIds.addAll(dataIds);
    }
    
    public void setIsAllDataRead(boolean isAllDataRead) {
        this.isAllDataRead &= isAllDataRead;
    }

}
