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
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.MaxRowsStatementCreator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * @see IIncomingBatchService
 */
public class IncomingBatchService extends AbstractService implements IIncomingBatchService {

    public IncomingBatch findIncomingBatch(long batchId, String nodeId) {
        try {
            return (IncomingBatch) jdbcTemplate.queryForObject(getSql("selectIncomingBatchPrefixSql","findIncomingBatchSql"), new Object[] { batchId,
                    nodeId }, new IncomingBatchMapper());
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
    
    public int countIncomingBatchesInError() {
        return jdbcTemplate.queryForInt(getSql("countIncomingBatchesErrorsSql"));
    }    

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {
        return (List<IncomingBatch>) jdbcTemplate.query(new MaxRowsStatementCreator(
                getSql("selectIncomingBatchPrefixSql","findIncomingBatchErrorsSql"), maxRows), new IncomingBatchMapper());
    }
    
    public List<Date> listIncomingBatchTimes(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime) {
        if (nodeIds != null && nodeIds.size() > 0 && channels != null && channels.size() > 0
                && statuses != null && statuses.size() > 0) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("NODES", nodeIds);
            params.put("CHANNELS", channels);
            params.put("STATUSES", toStringList(statuses));
            params.put("CREATE_TIME", startAtCreateTime);
            NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(jdbcTemplate);
            return template.query(getSql("selectCreateTimePrefixSql", containsOnlyErrorStatus(statuses) ? 
                    "listIncomingBatchesInErrorSql" : "listIncomingBatchesSql"),
                    params, new SingleColumnRowMapper<Date>());
        } else {
            return new ArrayList<Date>(0);
        }
    }
    
    public List<IncomingBatch> listIncomingBatches(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime, final int maxRowsToRetrieve) {
        if (nodeIds != null && nodeIds.size() > 0 && channels != null && channels.size() > 0
                && statuses != null && statuses.size() > 0) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("NODES", nodeIds);
            params.put("CHANNELS", channels);
            params.put("STATUSES", toStringList(statuses));
            params.put("CREATE_TIME", startAtCreateTime);

            NamedParameterJdbcTemplate template = new NamedParameterJdbcTemplate(jdbcTemplate);
            ResultSetExtractor<List<IncomingBatch>> extractor = new ResultSetExtractor<List<IncomingBatch>>() {
                IncomingBatchMapper rowMapper = new IncomingBatchMapper();

                public List<IncomingBatch> extractData(ResultSet rs) throws SQLException,
                        DataAccessException {
                    List<IncomingBatch> list = new ArrayList<IncomingBatch>(maxRowsToRetrieve);
                    int count = 0;
                    while (rs.next() && count < maxRowsToRetrieve) {
                        list.add(rowMapper.mapRow(rs, ++count));
                    }
                    return list;
                }
            };

            List<IncomingBatch> list = template.query(
                    getSql("selectIncomingBatchPrefixSql", containsOnlyErrorStatus(statuses) ? 
                            "listIncomingBatchesInErrorSql" : "listIncomingBatchesSql"),
                    new MapSqlParameterSource(params), extractor);
            return list;
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
            } catch (DataIntegrityViolationException e) {
                batch.setRetry(true);
                existingBatch = findIncomingBatch(batch.getBatchId(), batch.getNodeId());
            }

            if (batch.isRetry()) {
                if (existingBatch.getStatus() == Status.ER || existingBatch.getStatus() == Status.LD
                        || !parameterService
                                .is(ParameterConstants.INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED)) {
                    okayToProcess = true;
                    existingBatch.setStatus(Status.LD);
                    updateIncomingBatch(existingBatch);
                    log.warn("BatchRetrying", batch.getNodeBatchId());
                } else {
                    okayToProcess = false;
                    batch.setStatus(existingBatch.getStatus());
                    batch.setSkipCount(existingBatch.getSkipCount() + 1);
                    log.warn("BatchSkipping", batch.getNodeBatchId());
                }
            }
        }
        return okayToProcess;
    }

    public void insertIncomingBatch(IncomingBatch batch) {
        if (batch.isPersistable()) {
            batch.setLastUpdatedHostName(AppUtils.getServerId());
            batch.setLastUpdatedTime(new Date());
            jdbcTemplate.update(
                    getSql("insertIncomingBatchSql"),
                    new Object[] { Long.valueOf(batch.getBatchId()), batch.getNodeId(),
                            batch.getChannelId(), batch.getStatus().name(),
                            batch.getNetworkMillis(), batch.getFilterMillis(),
                            batch.getDatabaseMillis(), batch.getFailedRowNumber(),
                            batch.getByteCount(), batch.getStatementCount(),
                            batch.getFallbackInsertCount(), batch.getFallbackUpdateCount(),
                            batch.getMissingDeleteCount(), batch.getSkipCount(),
                            batch.getSqlState(), batch.getSqlCode(),
                            StringUtils.abbreviate(batch.getSqlMessage(), 1000),
                            batch.getLastUpdatedHostName(), batch.getLastUpdatedTime() });
        }
    }

    public int updateIncomingBatch(IncomingBatch batch) {
        return updateIncomingBatch(this.jdbcTemplate, batch);
    }
    
    public int updateIncomingBatch(JdbcTemplate template, IncomingBatch batch) {
        int count = 0;
        if (batch.isPersistable()) {
            if (batch.getStatus() == IncomingBatch.Status.ER) {
                batch.setErrorFlag(true);
            } else if (batch.getStatus() == IncomingBatch.Status.OK) {
                batch.setErrorFlag(false);
            }
            batch.setLastUpdatedHostName(AppUtils.getServerId());
            batch.setLastUpdatedTime(new Date());
            count = template.update(
                    getSql("updateIncomingBatchSql"),
                    new Object[] { batch.getStatus().name(), batch.isErrorFlag() ? 1 : 0, batch.getNetworkMillis(),
                            batch.getFilterMillis(), batch.getDatabaseMillis(),
                            batch.getFailedRowNumber(), batch.getByteCount(),
                            batch.getStatementCount(), batch.getFallbackInsertCount(),
                            batch.getFallbackUpdateCount(), batch.getMissingDeleteCount(),
                            batch.getSkipCount(), batch.getSqlState(), batch.getSqlCode(),
                            StringUtils.abbreviate(batch.getSqlMessage(), 1000),
                            batch.getLastUpdatedHostName(), batch.getLastUpdatedTime(),
                            Long.valueOf(batch.getBatchId()), batch.getNodeId() });
        }
        return count;
    }

    class IncomingBatchMapper implements RowMapper<IncomingBatch> {
        public IncomingBatch mapRow(ResultSet rs, int num) throws SQLException {
            IncomingBatch batch = new IncomingBatch();
            batch.setBatchId(rs.getLong(1));
            batch.setNodeId(rs.getString(2));
            batch.setChannelId(rs.getString(3));
            batch.setStatus(IncomingBatch.Status.valueOf(rs.getString(4)));            
            batch.setNetworkMillis(rs.getLong(5));
            batch.setFilterMillis(rs.getLong(6));
            batch.setDatabaseMillis(rs.getLong(7));
            batch.setFailedRowNumber(rs.getLong(8));
            batch.setByteCount(rs.getLong(9));
            batch.setStatementCount(rs.getLong(10));
            batch.setFallbackInsertCount(rs.getLong(11));
            batch.setFallbackUpdateCount(rs.getLong(12));
            batch.setMissingDeleteCount(rs.getLong(13));
            batch.setSkipCount(rs.getLong(14));
            batch.setSqlState(rs.getString(15));
            batch.setSqlCode(rs.getInt(16));
            batch.setSqlMessage(rs.getString(17));
            batch.setLastUpdatedHostName(rs.getString(18));
            batch.setLastUpdatedTime(rs.getTimestamp(19));
            batch.setCreateTime(rs.getTimestamp(20));
            batch.setErrorFlag(rs.getBoolean(21));
            return batch;
        }
    }

}