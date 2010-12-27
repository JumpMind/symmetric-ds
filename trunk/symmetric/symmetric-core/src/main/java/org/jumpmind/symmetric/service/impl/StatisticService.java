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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.ChannelStats;
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

    public TreeMap<Date, Map<String, ChannelStats>> getChannelStatsForPeriod(Date start, Date end, String nodeId, int periodSizeInMinutes) {
        TreeMap<Date, Map<String, ChannelStats>> orderedMap = new TreeMap<Date, Map<String,ChannelStats>>();
        Iterator<ChannelStats> stats = jdbcTemplate.query(getSql("selectChannelStatsSql"),
                new ChannelStatsMapper(), start, end, nodeId).iterator();
        Date periodStart = start;
        Date periodEnd = DateUtils.add(periodStart, Calendar.MINUTE, periodSizeInMinutes);
        orderedMap.put(periodStart, new HashMap<String, ChannelStats>());
        ChannelStats stat = null;
        while (periodStart.before(end)) {
            stat = stats.hasNext() && stat == null ? stats.next() : stat;
            if (stat != null && 
                    (periodStart.equals(stat.getStartTime()) || periodStart.before(stat.getStartTime())) 
                    && periodEnd.after(stat.getStartTime())) {
                Map<String, ChannelStats> map = orderedMap.get(periodStart);
                ChannelStats existing = map.get(stat.getChannelId());
                if (existing == null) {
                    map.put(stat.getChannelId(), stat);
                } else {
                    existing.add(stat); 
                }
                stat = null;
            } else {
                periodStart = periodEnd;
                periodEnd = DateUtils.add(periodStart, Calendar.MINUTE, periodSizeInMinutes);
                orderedMap.put(periodStart, new HashMap<String, ChannelStats>());
            }
        }
        return orderedMap;
    }

    class ChannelStatsMapper implements RowMapper<ChannelStats> {
        public ChannelStats mapRow(ResultSet rs, int rowNum) throws SQLException {
            ChannelStats stats = new ChannelStats();
            stats.setNodeId(rs.getString(1));
            stats.setHostName(rs.getString(2));
            stats.setChannelId(rs.getString(3));
            stats.setStartTime(DateUtils.truncate(rs.getTimestamp(4), Calendar.MILLISECOND));
            stats.setEndTime(DateUtils.truncate(rs.getTimestamp(5), Calendar.MILLISECOND));
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
}