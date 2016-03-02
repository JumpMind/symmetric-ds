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
package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.sql.mapper.DateMapper;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.BatchId;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.FormatUtils;

/**
 * @see IIncomingBatchService
 */
public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    protected IClusterService clusterService;
    
    @Override
    public List<String> getNodesInError() {
        return sqlTemplate.query(getSql("selectNodesInErrorSql"), new StringMapper());
    }
    
    public IncomingBatchService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IClusterService clusterService) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        setSqlMap(new IncomingBatchServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }
    
    public void refreshIncomingBatch(IncomingBatch batch) {
        sqlTemplate.queryForObject(
                getSql("selectIncomingBatchPrefixSql", "findIncomingBatchSql"),
                new IncomingBatchMapper(batch), batch.getBatchId(), batch.getNodeId());        
    }

    public IncomingBatch findIncomingBatch(long batchId, String nodeId) {
        if (nodeId != null) {
            return sqlTemplate.queryForObject(
                    getSql("selectIncomingBatchPrefixSql", "findIncomingBatchSql"),
                    new IncomingBatchMapper(), batchId, nodeId);
        } else {
            return sqlTemplate.queryForObject(
                    getSql("selectIncomingBatchPrefixSql", "findIncomingBatchByBatchIdSql"),
                    new IncomingBatchMapper(), batchId);            
        }
    }

    public int countIncomingBatchesInError() {
        return sqlTemplate.queryForInt(getSql("countIncomingBatchesErrorsSql"));
    }
    
    public int countIncomingBatchesInError(String channelId) {
        return sqlTemplate.queryForInt(getSql("countIncomingBatchesErrorsOnChannelSql"), channelId);
    }

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {
        return sqlTemplate.query(
                getSql("selectIncomingBatchPrefixSql", "findIncomingBatchErrorsSql"), maxRows,
                new IncomingBatchMapper());
    }

    public void markIncomingBatchesOk(String nodeId) {
        List<IncomingBatch> batches = listIncomingBatchesInErrorFor(nodeId);
        for (IncomingBatch incomingBatch : batches) {
            if (isRecordOkBatchesEnabled()) {
                incomingBatch.setErrorFlag(false);
                incomingBatch.setStatus(Status.OK);
                updateIncomingBatch(incomingBatch);
            } else {
                deleteIncomingBatch(incomingBatch);
            }
        }
    }

    public void removingIncomingBatches(String nodeId) {
        sqlTemplate.update(getSql("deleteIncomingBatchByNodeSql"), nodeId);
    }

    public List<IncomingBatch> listIncomingBatchesInErrorFor(String nodeId) {
        return sqlTemplate.query(
                getSql("selectIncomingBatchPrefixSql", "listIncomingBatchesInErrorForNodeSql"),
                new IncomingBatchMapper(), nodeId);
    }
    
    @SuppressWarnings("deprecation")
    public boolean isRecordOkBatchesEnabled() {
        boolean enabled = true;        
        if (!parameterService.is(ParameterConstants.INCOMING_BATCH_RECORD_OK_ENABLED, true)) {
            enabled = false;
        }
        if (parameterService.is(ParameterConstants.INCOMING_BATCH_DELETE_ON_LOAD, false)) {
            enabled = false;
        }
        return enabled;
    } 

    public List<Date> listIncomingBatchTimes(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, boolean ascending) {

        String whereClause = buildBatchWhere(nodeIds, channels, statuses);

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODES", nodeIds);
        params.put("CHANNELS", channels);
        params.put("STATUSES", toStringList(statuses));

        String sql = getSql("selectCreateTimePrefixSql", whereClause,
                ascending ? " order by create_time" : " order by create_time desc");
        return sqlTemplate.query(sql, new DateMapper(), params);
    }

    public List<IncomingBatch> listIncomingBatches(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime,
            final int maxRowsToRetrieve, boolean ascending) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("NODES", nodeIds);
            params.put("CHANNELS", channels);
            params.put("STATUSES", toStringList(statuses));
            params.put("CREATE_TIME", startAtCreateTime);
            
            String where = buildBatchWhere(nodeIds, channels, statuses);

            String createTimeLimiter = "";
            if (startAtCreateTime != null) {
                if (StringUtils.isBlank(where)) {
                    where = " where 1=1 ";
                }
                createTimeLimiter = " and create_time " + (ascending ? ">=" : "<=")
                        + " :CREATE_TIME";
            }

            String sql = getSql("selectIncomingBatchPrefixSql", where, createTimeLimiter,
                    ascending ? " order by create_time" : " order by create_time desc");

            return sqlTemplate.query(sql, maxRowsToRetrieve, new IncomingBatchMapper(), params);

    }

    protected boolean containsOnlyErrorStatus(List<IncomingBatch.Status> statuses) {
        return statuses.size() == 1 && statuses.get(0) == IncomingBatch.Status.ER;
    }

    protected List<String> toStringList(List<IncomingBatch.Status> statuses) {
        List<String> statusStrings = new ArrayList<String>(statuses.size());
        for (Status status : statuses) {
            statusStrings.add(status.name());
        }
        return statusStrings;

    }

    public boolean acquireIncomingBatch(IncomingBatch batch) {
        boolean okayToProcess = true;
        if (batch.isPersistable()) {
            IncomingBatch existingBatch = null;

            if (isRecordOkBatchesEnabled()) {
                try {
                    insertIncomingBatch(batch);
                } catch (UniqueKeyException e) {
                    batch.setRetry(true);
                    existingBatch = findIncomingBatch(batch.getBatchId(), batch.getNodeId());
                }
            } else {                
                existingBatch = findIncomingBatch(batch.getBatchId(), batch.getNodeId());
                if (existingBatch != null) {
                    batch.setRetry(true);
                }
            }

            if (batch.isRetry()) {
                if (existingBatch.getStatus() == Status.ER
                        || existingBatch.getStatus() == Status.LD
                        || !parameterService
                                .is(ParameterConstants.INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED)) {
                    okayToProcess = true;
                    existingBatch.setStatus(Status.LD);
                    log.info("Retrying batch {}", batch.getNodeBatchId());
                } else if (existingBatch.getStatus() == Status.IG) {
                    okayToProcess = false;
                    batch.setStatus(Status.OK);
                    batch.incrementIgnoreCount();
                    existingBatch.setStatus(Status.OK);
                    existingBatch.incrementIgnoreCount();
                    log.info("Ignoring batch {}", batch.getNodeBatchId());
                } else {
                    okayToProcess = false;
                    batch.setStatus(existingBatch.getStatus());
                    batch.setByteCount(existingBatch.getByteCount());
                    batch.setDatabaseMillis(existingBatch.getDatabaseMillis());
                    batch.setNetworkMillis(existingBatch.getNetworkMillis());
                    batch.setFilterMillis(existingBatch.getFilterMillis());
                    batch.setSkipCount(existingBatch.getSkipCount() + 1);
                    batch.setStatementCount(existingBatch.getStatementCount());

                    existingBatch.setSkipCount(existingBatch.getSkipCount() + 1);
                    log.info("Skipping batch {}", batch.getNodeBatchId());
                }
                updateIncomingBatch(existingBatch);
            }
        }
        return okayToProcess;
    }
    
    public void insertIncomingBatch(ISqlTransaction transaction, IncomingBatch batch) {
        if (batch.isPersistable()) {
        	boolean alreadyExists = false;
        	if (symmetricDialect.getName().equals(DatabaseNamesConstants.REDSHIFT)) {
        		if (findIncomingBatch(batch.getBatchId(), batch.getNodeId()) != null) {
        			alreadyExists = true;
        		}
        	}
        	if (!alreadyExists) {
	        	batch.setLastUpdatedHostName(clusterService.getServerId());
	            batch.setLastUpdatedTime(new Date());
	            transaction.prepareAndExecute(
	                    getSql("insertIncomingBatchSql"),
	                    new Object[] { batch.getBatchId(), batch.getNodeId(), batch.getChannelId(),
	                            batch.getStatus().name(), batch.getNetworkMillis(),
	                            batch.getFilterMillis(), batch.getDatabaseMillis(),
	                            batch.getFailedRowNumber(), batch.getFailedLineNumber(),
	                            batch.getByteCount(), batch.getStatementCount(),
	                            batch.getFallbackInsertCount(), batch.getFallbackUpdateCount(),
	                            batch.getIgnoreCount(), batch.getMissingDeleteCount(),
	                            batch.getSkipCount(), batch.getSqlState(), batch.getSqlCode(),
	                            FormatUtils.abbreviateForLogging(batch.getSqlMessage()),
	                            batch.getLastUpdatedHostName(), batch.getLastUpdatedTime() },
	                    new int[] { Types.NUMERIC, Types.VARCHAR, Types.VARCHAR, Types.CHAR,
	                            Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
	                            Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
	                            Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
	                            Types.VARCHAR, Types.NUMERIC, Types.VARCHAR, Types.VARCHAR,
	                            Types.TIMESTAMP });
        	}
        }
    }
    
    public void insertIncomingBatch(IncomingBatch batch) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertIncomingBatch(transaction, batch);
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
            close(transaction);
        }    
    }

    public int deleteIncomingBatch(IncomingBatch batch) {
        return sqlTemplate.update(getSql("deleteIncomingBatchSql"),
                new Object[] { batch.getBatchId(), batch.getNodeId() }, new int[] { symmetricDialect.getSqlTypeForIds(),
                        Types.VARCHAR });
    }
    
    public int updateIncomingBatch(IncomingBatch batch) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            int count = updateIncomingBatch(transaction, batch);
            transaction.commit();
            return count;   
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
            close(transaction);
        }        
    }

    public int updateIncomingBatch(ISqlTransaction transaction , IncomingBatch batch) {
        int count = 0;
        if (batch.isPersistable()) {
            if (batch.getStatus() == IncomingBatch.Status.ER) {
                batch.setErrorFlag(true);
            } else if (batch.getStatus() == IncomingBatch.Status.OK) {
                batch.setErrorFlag(false);
            }
            batch.setLastUpdatedHostName(clusterService.getServerId());
            batch.setLastUpdatedTime(new Date());
            count =  transaction.prepareAndExecute(
                    getSql("updateIncomingBatchSql"),
                    new Object[] { batch.getStatus().name(), batch.isErrorFlag() ? 1 : 0,
                            batch.getNetworkMillis(), batch.getFilterMillis(),
                            batch.getDatabaseMillis(), batch.getFailedRowNumber(),
                            batch.getFailedLineNumber(), batch.getByteCount(),
                            batch.getStatementCount(), batch.getFallbackInsertCount(),
                            batch.getFallbackUpdateCount(), batch.getIgnoreCount(),
                            batch.getMissingDeleteCount(), batch.getSkipCount(),
                            batch.getSqlState(), batch.getSqlCode(),
                            FormatUtils.abbreviateForLogging(batch.getSqlMessage()),
                            batch.getLastUpdatedHostName(), batch.getLastUpdatedTime(),
                            batch.getBatchId(), batch.getNodeId() }, new int[] { Types.CHAR,
                            Types.SMALLINT, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                            Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                            Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                            Types.NUMERIC, Types.VARCHAR, Types.NUMERIC, Types.VARCHAR,
                            Types.VARCHAR, Types.TIMESTAMP, symmetricDialect.getSqlTypeForIds(), Types.VARCHAR });
        }
        return count;
    }
    
    public Map<String, BatchId> findMaxBatchIdsByChannel() {
        Map<String, BatchId> ids = new HashMap<String, BatchId>();
        sqlTemplate.query(getSql("maxBatchIdsSql"), new BatchIdMapper(ids),
                IncomingBatch.Status.OK.name());
        return ids;
    }

    class BatchIdMapper implements ISqlRowMapper<BatchId> {
        Map<String, BatchId> ids;

        public BatchIdMapper(Map<String, BatchId> ids) {
            this.ids = ids;
        }

        public BatchId mapRow(Row rs) {
            BatchId batch = new BatchId();
            batch.setBatchId(rs.getLong("batch_id"));
            batch.setNodeId(rs.getString("node_id"));
            ids.put(rs.getString("channel_id"), batch);
            return batch;
        }
    }

    class IncomingBatchMapper implements ISqlRowMapper<IncomingBatch> {
        
        IncomingBatch batchToRefresh = null;
        
        public IncomingBatchMapper(IncomingBatch batchToRefresh) {
            this.batchToRefresh = batchToRefresh;    
        }
        
        public IncomingBatchMapper() {
        }
        
        
        public IncomingBatch mapRow(Row rs) {
            IncomingBatch batch = batchToRefresh != null ? batchToRefresh : new IncomingBatch();
            batch.setBatchId(rs.getLong("batch_id"));
            batch.setNodeId(rs.getString("node_id"));
            batch.setChannelId(rs.getString("channel_id"));
            batch.setStatus(rs.getString("status"));
            batch.setNetworkMillis(rs.getLong("network_millis"));
            batch.setFilterMillis(rs.getLong("filter_millis"));
            batch.setDatabaseMillis(rs.getLong("database_millis"));
            batch.setFailedRowNumber(rs.getLong("failed_row_number"));
            batch.setFailedLineNumber(rs.getLong("failed_line_number"));
            batch.setByteCount(rs.getLong("byte_count"));
            batch.setStatementCount(rs.getLong("statement_count"));
            batch.setFallbackInsertCount(rs.getLong("fallback_insert_count"));
            batch.setFallbackUpdateCount(rs.getLong("fallback_update_count"));
            batch.setIgnoreCount(rs.getLong("ignore_count"));
            batch.setMissingDeleteCount(rs.getLong("missing_delete_count"));
            batch.setSkipCount(rs.getLong("skip_count"));
            batch.setSqlState(rs.getString("sql_state"));
            batch.setSqlCode(rs.getInt("sql_code"));
            batch.setSqlMessage(rs.getString("sql_message"));
            batch.setLastUpdatedHostName(rs.getString("last_update_hostname"));
            batch.setLastUpdatedTime(rs.getDateTime("last_update_time"));
            batch.setCreateTime(rs.getDateTime("create_time"));
            batch.setErrorFlag(rs.getBoolean("error_flag"));
            return batch;
        }
    }

}
