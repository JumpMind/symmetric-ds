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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.ChannelStats;
import org.jumpmind.symmetric.statistic.ChannelStatsByPeriodMap;
import org.jumpmind.symmetric.statistic.HostStats;
import org.jumpmind.symmetric.statistic.HostStatsByPeriodMap;
import org.jumpmind.symmetric.statistic.JobStats;
import org.springframework.jdbc.core.RowMapper;

/**
 * @see IStatisticService
 */
public class StatisticService extends AbstractService implements IStatisticService {

    public void save(ChannelStats stats) {
        jdbcTemplate.update(
                getSql("insertChannelStatsSql"),
                new Object[] { stats.getNodeId(), stats.getHostName(), stats.getChannelId(),
                        stats.getStartTime(), stats.getEndTime(), stats.getDataRouted(),
                        stats.getDataUnRouted(), stats.getDataEventInserted(),
                        stats.getDataExtracted(), stats.getDataBytesExtracted(),
                        stats.getDataExtractedErrors(), stats.getDataSent(),
                        stats.getDataBytesSent(), stats.getDataSentErrors(), stats.getDataLoaded(),
                        stats.getDataBytesLoaded(), stats.getDataLoadedErrors() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT });
    }
    
    public void save(JobStats stats) {
        jdbcTemplate.update(
                getSql("insertJobStatsSql"),
                new Object[] { stats.getNodeId(), stats.getHostName(), stats.getJobName(),
                        stats.getStartTime(), stats.getEndTime(), stats.getProcessedCount() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.BIGINT }); 
    }
    
    public List<JobStats> getJobStatsForPeriod(Date start, Date end,
            String nodeId) {
        return jdbcTemplate.query(getSql("selectChannelStatsSql"),
                new JobStatsMapper(), start, end, nodeId);        
    }  

    public TreeMap<Date, Map<String, ChannelStats>> getChannelStatsForPeriod(Date start, Date end,
            String nodeId, int periodSizeInMinutes) {
        List<ChannelStats> list = jdbcTemplate.query(getSql("selectChannelStatsSql"),
                new ChannelStatsMapper(), start, end, nodeId);        
        return new ChannelStatsByPeriodMap(start, end, list, periodSizeInMinutes);
    }    

    public void save(HostStats stats) {
        jdbcTemplate.update(
                getSql("insertHostStatsSql"),
                new Object[] { stats.getNodeId(), stats.getHostName(), stats.getStartTime(),
                        stats.getEndTime(), stats.getRestarted(), stats.getNodesPulled(),
                        stats.getNodesPushed(), stats.getNodesRejected(),
                        stats.getNodesRegistered(), stats.getNodesLoaded(),
                        stats.getNodesDisabled(), stats.getPurgedDataRows(),
                        stats.getPurgedDataEventRows(), stats.getPurgedBatchOutgoingRows(),
                        stats.getPurgedBatchIncomingRows(), stats.getTriggersCreatedCount(),
                        stats.getTriggersRebuiltCount(), stats.getTriggersRemovedCount(),
                        stats.getTotalNodesPullTime(), stats.getTotalNodesPushTime()},
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.TIMESTAMP,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, 
                        Types.BIGINT });
    }
    

    public TreeMap<Date, HostStats> getHostStatsForPeriod(Date start, Date end, String nodeId,
            int periodSizeInMinutes) {
        List<HostStats> list = jdbcTemplate.query(getSql("selectHostStatsSql"),
                new HostStatsMapper(), start, end, nodeId);
        return new HostStatsByPeriodMap(start, end, list, periodSizeInMinutes);
    }    
    
    public Date truncateToMinutes(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        return cal.getTime();        
    }
    
    class JobStatsMapper implements RowMapper<JobStats> {
        public JobStats mapRow(ResultSet rs, int rowNum) throws SQLException {
            JobStats stats = new JobStats();
            stats.setNodeId(rs.getString(1));
            stats.setHostName(rs.getString(2));
            stats.setJobName(rs.getString(3));
            stats.setStartTime(truncateToMinutes(rs.getTimestamp(4)));
            stats.setEndTime(truncateToMinutes(rs.getTimestamp(5)));
            stats.setProcessedCount(rs.getLong(6));
            return stats;
        }
    }

    class ChannelStatsMapper implements RowMapper<ChannelStats> {
        public ChannelStats mapRow(ResultSet rs, int rowNum) throws SQLException {
            ChannelStats stats = new ChannelStats();
            stats.setNodeId(rs.getString(1));
            stats.setHostName(rs.getString(2));
            stats.setChannelId(rs.getString(3));
            stats.setStartTime(truncateToMinutes(rs.getTimestamp(4)));
            stats.setEndTime(truncateToMinutes(rs.getTimestamp(5)));
            stats.setDataRouted(rs.getLong(6));
            stats.setDataUnRouted(rs.getLong(7));
            stats.setDataEventInserted(rs.getLong(8));
            stats.setDataExtracted(rs.getLong(9));
            stats.setDataBytesExtracted(rs.getLong(10));
            stats.setDataExtractedErrors(rs.getLong(11));
            stats.setDataSent(rs.getLong(12));
            stats.setDataBytesSent(rs.getLong(13));
            stats.setDataSentErrors(rs.getLong(14));
            stats.setDataLoaded(rs.getLong(15));
            stats.setDataBytesLoaded(rs.getLong(16));
            stats.setDataLoadedErrors(rs.getLong(17));
            return stats;
        }
    }

    class HostStatsMapper implements RowMapper<HostStats> {
        public HostStats mapRow(ResultSet rs, int rowNum) throws SQLException {
            HostStats stats = new HostStats();
            stats.setNodeId(rs.getString(1));
            stats.setHostName(rs.getString(2));            
            stats.setStartTime(truncateToMinutes(rs.getTimestamp(3)));
            stats.setEndTime(truncateToMinutes(rs.getTimestamp(4)));
            stats.setRestarted(rs.getLong(5));
            stats.setNodesPulled(rs.getLong(6));
            stats.setNodesPushed(rs.getLong(7));
            stats.setNodesRejected(rs.getLong(8));
            stats.setNodesRegistered(rs.getLong(9));
            stats.setNodesLoaded(rs.getLong(10));
            stats.setNodesDisabled(rs.getLong(11));
            stats.setPurgedDataRows(rs.getLong(12));
            stats.setPurgedDataEventRows(rs.getLong(13));
            stats.setPurgedBatchOutgoingRows(rs.getLong(14));
            stats.setPurgedBatchIncomingRows(rs.getLong(15));
            stats.setTriggersCreatedCount(rs.getLong(16));
            stats.setTriggersRebuiltCount(rs.getLong(17));
            stats.setTriggersRemovedCount(rs.getLong(18));
            stats.setTotalNodesPullTime(rs.getLong(19));
            stats.setTotalNodesPushTime(rs.getLong(20));
            return stats;
        }
    }
}