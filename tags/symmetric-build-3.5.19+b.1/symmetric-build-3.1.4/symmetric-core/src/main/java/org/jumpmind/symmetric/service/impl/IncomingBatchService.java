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
package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.sql.mapper.DateMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.util.FormatUtils;

/**
 * @see IIncomingBatchService
 */
public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    public IncomingBatchService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect) {
        super(parameterService, symmetricDialect);
        setSqlMap(new IncomingBatchServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public IncomingBatch findIncomingBatch(long batchId, String nodeId) {
        return sqlTemplate.queryForObject(
                getSql("selectIncomingBatchPrefixSql", "findIncomingBatchSql"),
                new IncomingBatchMapper(), batchId, nodeId);
    }

    public int countIncomingBatchesInError() {
        return sqlTemplate.queryForInt(getSql("countIncomingBatchesErrorsSql"));
    }

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {
        return sqlTemplate.query(
                getSql("selectIncomingBatchPrefixSql", "findIncomingBatchErrorsSql"), maxRows,
                new IncomingBatchMapper());
    }

    public void markIncomingBatchesOk(String nodeId) {
        List<IncomingBatch> batches = listIncomingBatchesInErrorFor(nodeId);
        for (IncomingBatch incomingBatch : batches) {
            incomingBatch.setErrorFlag(false);
            incomingBatch.setStatus(Status.OK);
            updateIncomingBatch(incomingBatch);
        }
    }

    public List<IncomingBatch> listIncomingBatchesInErrorFor(String nodeId) {
        return sqlTemplate.query(
                getSql("selectIncomingBatchPrefixSql", "listIncomingBatchesInErrorForNodeSql"),
                new IncomingBatchMapper(), nodeId);
    }

    public List<Date> listIncomingBatchTimes(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, boolean ascending) {
        if (nodeIds != null && nodeIds.size() > 0 && channels != null && channels.size() > 0
                && statuses != null && statuses.size() > 0) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("NODES", nodeIds);
            params.put("CHANNELS", channels);
            params.put("STATUSES", toStringList(statuses));
            String sql = getSql("selectCreateTimePrefixSql",
                    containsOnlyErrorStatus(statuses) ? "listIncomingBatchesInErrorSql"
                            : "listIncomingBatchesSql", ascending ? " order by create_time"
                            : " order by create_time desc");
            return sqlTemplate.query(sql, new DateMapper(), params);
        } else {
            return new ArrayList<Date>(0);
        }
    }

    public List<IncomingBatch> listIncomingBatches(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime,
            final int maxRowsToRetrieve, boolean ascending) {
        if (nodeIds != null && nodeIds.size() > 0 && channels != null && channels.size() > 0
                && statuses != null && statuses.size() > 0) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("NODES", nodeIds);
            params.put("CHANNELS", channels);
            params.put("STATUSES", toStringList(statuses));
            params.put("CREATE_TIME", startAtCreateTime);

            String createTimeLimiter = "";
            if (startAtCreateTime != null) {
                createTimeLimiter = " and create_time " + (ascending ? ">=" : "<=")
                        + " :CREATE_TIME";
            }

            String sql = getSql("selectIncomingBatchPrefixSql",
                    containsOnlyErrorStatus(statuses) ? "listIncomingBatchesInErrorSql"
                            : "listIncomingBatchesSql", createTimeLimiter,
                    ascending ? " order by create_time" : " order by create_time desc");

            return sqlTemplate.query(sql, maxRowsToRetrieve, new IncomingBatchMapper(), params);
        } else {
            return new ArrayList<IncomingBatch>(0);
        }
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

            try {
                insertIncomingBatch(batch);
            } catch (UniqueKeyException e) {
                batch.setRetry(true);
                existingBatch = findIncomingBatch(batch.getBatchId(), batch.getNodeId());
            }

            if (batch.isRetry()) {
                if (existingBatch.getStatus() == Status.ER
                        || existingBatch.getStatus() == Status.LD
                        || !parameterService
                                .is(ParameterConstants.INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED)) {
                    okayToProcess = true;
                    existingBatch.setStatus(Status.LD);
                    log.warn("Retrying batch {}", batch.getNodeBatchId());
                } else if (existingBatch.getStatus() == Status.IG) {
                    okayToProcess = false;
                    batch.setStatus(Status.OK);
                    batch.incrementIgnoreCount();
                    log.warn("Ignoring batch {}", batch.getNodeBatchId());
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
                    log.warn("Skipping batch {}", batch.getNodeBatchId());
                }
                updateIncomingBatch(existingBatch);
            }
        }
        return okayToProcess;
    }

    public void insertIncomingBatch(IncomingBatch batch) {
        if (batch.isPersistable()) {
            batch.setLastUpdatedHostName(AppUtils.getServerId());
            batch.setLastUpdatedTime(new Date());
            sqlTemplate.update(
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

    public int deleteIncomingBatch(IncomingBatch batch) {
        return sqlTemplate.update(getSql("deleteIncomingBatchSql"),
                new Object[] { batch.getBatchId(), batch.getNodeId() }, new int[] { Types.NUMERIC,
                        Types.VARCHAR });
    }

    public int updateIncomingBatch(IncomingBatch batch) {
        int count = 0;
        if (batch.isPersistable()) {
            if (batch.getStatus() == IncomingBatch.Status.ER) {
                batch.setErrorFlag(true);
            } else if (batch.getStatus() == IncomingBatch.Status.OK) {
                batch.setErrorFlag(false);
            }
            batch.setLastUpdatedHostName(AppUtils.getServerId());
            batch.setLastUpdatedTime(new Date());
            count = sqlTemplate.update(
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
                            Types.VARCHAR, Types.TIMESTAMP, Types.NUMERIC, Types.VARCHAR });
        }
        return count;
    }

    class IncomingBatchMapper implements ISqlRowMapper<IncomingBatch> {
        public IncomingBatch mapRow(Row rs) {
            IncomingBatch batch = new IncomingBatch();
            batch.setBatchId(rs.getLong("batch_id"));
            batch.setNodeId(rs.getString("node_id"));
            batch.setChannelId(rs.getString("channel_id"));
            batch.setStatus(IncomingBatch.Status.valueOf(rs.getString("status")));
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
