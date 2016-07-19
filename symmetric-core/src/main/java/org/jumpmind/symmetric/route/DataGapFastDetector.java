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
    
    protected boolean isAllDataRead = true;
    
    protected long maxDataToSelect;
    
    protected boolean isBusyExpire;
        
    protected Set<DataGap> gapsAll;
    
    protected Set<DataGap> gapsAdded;
    
    protected Set<DataGap> gapsDeleted;

    public DataGapFastDetector(IDataService dataService, IParameterService parameterService, IContextService contextService,
            ISymmetricDialect symmetricDialect, IRouterService routerService, IStatisticManager statisticManager, INodeService nodeService) {
        this.dataService = dataService;
        this.parameterService = parameterService;
        this.contextService = contextService;
        this.routerService = routerService;
        this.symmetricDialect = symmetricDialect;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
        reset();
    }

    public void beforeRouting() {
        ProcessInfo processInfo = this.statisticManager.newProcessInfo(new ProcessInfoKey(
                nodeService.findIdentityNodeId(), null, ProcessType.GAP_DETECT));
        processInfo.setStatus(Status.QUERYING);
        maxDataToSelect = parameterService.getLong(ParameterConstants.ROUTING_LARGEST_GAP_SIZE);
        gaps = dataService.findDataGaps();
        processInfo.setStatus(Status.OK);
        fixOverlappingGaps(gaps, processInfo);

        if (contextService.is(ContextConstants.ROUTING_FULL_GAP_ANALYSIS)) {
            log.info("Full gap analysis is running");
            long ts = System.currentTimeMillis();
            queryDataIdMap();
            log.info("Querying data in gaps from database took {} ms", System.currentTimeMillis() - ts);
            afterRouting();
            log.info("Full gap analysis is done after {} ms", System.currentTimeMillis() - ts);
            gaps = dataService.findDataGaps();
            reset();
        }
    }

    protected void reset() {
        dataIds = new ArrayList<Long>();
        gapsAll = new HashSet<DataGap>();
        gapsAdded = new HashSet<DataGap>();
        gapsDeleted = new HashSet<DataGap>();
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

        long currentTime = System.currentTimeMillis();
        ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();

        boolean supportsTransactionViews = symmetricDialect.supportsTransactionViews();
        long earliestTransactionTime = 0;
        if (supportsTransactionViews) {
            Date date = symmetricDialect.getEarliestTransactionStartTime();
            if (date != null) {
                earliestTransactionTime = date.getTime() - parameterService.getLong(
                        ParameterConstants.DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS, 60000);
            }
            currentTime = symmetricDialect.getDatabaseTime();
        }

        Date currentDate = new Date(currentTime);
        if (!isAllDataRead) {
            long lastBusyExpireRunTime = contextService.getLong(ContextConstants.ROUTING_LAST_BUSY_EXPIRE_RUN_TIME);
            long busyExpireMillis = parameterService.getLong(ParameterConstants.ROUTING_STALE_GAP_BUSY_EXPIRE_TIME);
            isBusyExpire = lastBusyExpireRunTime == 0 || System.currentTimeMillis() - lastBusyExpireRunTime >= busyExpireMillis;
        }

        try {
            long ts = System.currentTimeMillis();
            long lastDataId = -1;
            int dataIdCount = 0;
            int rangeChecked = 0;
            int expireChecked = 0;
            gapsAll.addAll(gaps);
            Map<DataGap, List<Long>> dataIdMap = getDataIdMap();

            for (final DataGap dataGap : gaps) {
                final boolean lastGap = dataGap.equals(gaps.get(gaps.size() - 1));
                lastDataId = -1;
                List<Long> ids = dataIdMap.get(dataGap);
                
                dataIdCount += ids.size();
                rangeChecked += dataGap.getEndId() - dataGap.getStartId();
                
                ISqlTransaction transaction = null;
                try {
                    transaction = sqlTemplate.startSqlTransaction();

                    // if we found data in the gap
                    if (ids.size() > 0) {
                        dataService.deleteDataGap(transaction, dataGap);
                        gapsDeleted.add(dataGap);
                        gapsAll.remove(dataGap);

                    // if we did not find data in the gap and it was not the last gap
                    } else if (!lastGap && (isAllDataRead || isBusyExpire)) {
                        Date createTime = dataGap.getCreateTime();
                        boolean isExpired = false;
                        if (supportsTransactionViews) {
                            isExpired = createTime != null && (createTime.getTime() < earliestTransactionTime || earliestTransactionTime == 0);
                        } else {
                            isExpired = createTime != null && currentTime - createTime.getTime() > gapTimoutInMs;
                        }

                        if (isExpired) {
                            boolean isGapEmpty = false;
                            if (!isAllDataRead) {
                                isGapEmpty = dataService.countDataInRange(dataGap.getStartId() - 1, dataGap.getEndId() + 1) == 0;
                                expireChecked++;
                            }
                            if (isAllDataRead || isGapEmpty) {
                                if (dataGap.getStartId() == dataGap.getEndId()) {
                                    log.info("Found a gap in data_id at {}.  Skipping it because " +
                                            (supportsTransactionViews ? "there are no pending transactions" : "the gap expired"), dataGap.getStartId());
                                } else {
                                    log.info("Found a gap in data_id from {} to {}.  Skipping it because " +
                                            (supportsTransactionViews ? "there are no pending transactions" : "the gap expired"), 
                                            dataGap.getStartId(), dataGap.getEndId());
                                }
                                dataService.deleteDataGap(transaction, dataGap);
                                gapsDeleted.add(dataGap);
                                gapsAll.remove(dataGap);
                            }
                        }
                    }

                    for (Number number : ids) {
                        long dataId = number.longValue();
                        processInfo.incrementCurrentDataCount();
                        if (lastDataId == -1 && dataGap.getStartId() + dataIdIncrementBy <= dataId) {
                            // there was a new gap at the start
                            DataGap newGap = new DataGap(dataGap.getStartId(), dataId - 1, currentDate);
                            if (isOkayToAdd(newGap)) {
                                dataService.insertDataGap(transaction, newGap);
                            }
                        } else if (lastDataId != -1 && lastDataId + dataIdIncrementBy != dataId && lastDataId != dataId) {
                            // found a gap somewhere in the existing gap
                            DataGap newGap = new DataGap(lastDataId + 1, dataId - 1, currentDate);
                            if (isOkayToAdd(newGap)) {
                                dataService.insertDataGap(transaction, newGap);
                            }
                        }
                        lastDataId = dataId;
                    }

                    // if we found data in the gap
                    if (lastDataId != -1 && !lastGap && lastDataId + dataIdIncrementBy <= dataGap.getEndId()) {
                        DataGap newGap = new DataGap(lastDataId + dataIdIncrementBy, dataGap.getEndId(), currentDate);
                        if (isOkayToAdd(newGap)) {
                            dataService.insertDataGap(transaction, newGap);
                        }
                    }
                    
                    if (System.currentTimeMillis() - printStats > 30000) {
                        log.info(
                                "The data gap detection process has been running for {}ms, detected {} rows that have been previously routed over a total gap range of {}, "
                                        + "inserted {} new gaps, deleted {} gaps, and checked data in {} gaps", new Object[] { System.currentTimeMillis() - ts,
                                        dataIdCount, rangeChecked, gapsAdded.size(), gapsDeleted.size(), expireChecked });
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
                DataGap newGap = new DataGap(lastDataId + 1, lastDataId + maxDataToSelect, currentDate);
                if (isOkayToAdd(newGap)) {
                    log.debug("Inserting new last data gap: {}", newGap);
                    dataService.insertDataGap(newGap);
                }
            }

            contextService.save(ContextConstants.ROUTING_FULL_GAP_ANALYSIS, "false");
            if (!isAllDataRead && expireChecked > 0) {
                contextService.save(ContextConstants.ROUTING_LAST_BUSY_EXPIRE_RUN_TIME, String.valueOf(System.currentTimeMillis()));
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

    protected boolean isOkayToAdd(DataGap dataGap) {
        boolean isOkay = true;
        if (gapsAll.contains(dataGap)) {
            log.warn("Detected a duplicate data gap: " + dataGap);
            isOkay = false;
        } else if (dataGap.getStartId() > dataGap.getEndId()) {
            log.warn("Detected an invalid gap range: " + dataGap);
            isOkay = false;
        } else if (dataGap.gapSize() < maxDataToSelect - 1 && dataGap.gapSize() >= (long) (maxDataToSelect * 0.75)) {
            log.warn("Detected a very large gap range: " + dataGap);
            isOkay = false;
        }

        if (isOkay) {
            gapsAdded.add(dataGap);
            gapsAll.add(dataGap);
        } else {
            log.info("Data IDs: " + dataIds.toString());
            log.info("Data Gaps: " + gaps.toString());
            log.info("Added Data Gaps: " + gapsAdded.toString());
            log.info("Deleted Data Gaps: " + gapsDeleted.toString());            
        }
        return isOkay;
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

        return map;
    }
    
    protected void fixOverlappingGaps(List<DataGap> gaps, ProcessInfo processInfo) {
        try {
            ISqlTransaction transaction = null;
            log.debug("Looking for overlapping gaps");
            try {
                ISqlTemplate sqlTemplate = symmetricDialect.getPlatform().getSqlTemplate();
                transaction = sqlTemplate.startSqlTransaction();
                DataGap prevGap = null, lastGap = null;
                for (int i = 0; i < gaps.size(); i++) {
                    DataGap curGap = gaps.get(i);
                    if (lastGap != null) {
                        log.warn("Removing gap found after last gap: " + curGap);
                        dataService.deleteDataGap(transaction, curGap);
                        gaps.remove(i--);
                    } else {
                        if (lastGap == null && curGap.gapSize() >= maxDataToSelect - 1) {
                            lastGap = curGap;
                        }

                        if (prevGap != null) {
                            if (prevGap.overlaps(curGap)) {
                                log.warn("Removing overlapping gaps: " + prevGap + ", " + curGap);
                                dataService.deleteDataGap(transaction, prevGap);
                                dataService.deleteDataGap(transaction, curGap);
                                DataGap newGap = null;
                                if (curGap.equals(lastGap)) {
                                    newGap = new DataGap(prevGap.getStartId(), prevGap.getStartId() + maxDataToSelect - 1);
                                } else {
                                    newGap = new DataGap(prevGap.getStartId(), 
                                            prevGap.getEndId() > curGap.getEndId() ? prevGap.getEndId() : curGap.getEndId());
                                }
                                log.warn("Inserting new gap to fix overlap: " + newGap);
                                dataService.insertDataGap(transaction, newGap);
                                gaps.remove(i--);
                                gaps.set(i, newGap);
                                curGap = newGap;
                            }
                        }
                    }
                    prevGap = curGap;
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
        } catch (RuntimeException ex) {
            processInfo.setStatus(Status.ERROR);
            throw ex;
        }
    }

    public Long mapRow(Row row) {
        return row.getLong("data_id");
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
