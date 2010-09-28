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

import java.sql.Types;

import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.ChannelStats;

/**
 * 
 */
public class StatisticService extends AbstractService implements IStatisticService {

    public void save(ChannelStats stats) {
        jdbcTemplate.update(getSql("insertChannelStatsSql"), new Object[] { stats.getNodeId(),
                stats.getHostName(), stats.getChannelId(), stats.getStartTime(),
                stats.getEndTime(), stats.getDataRouted(), stats.getDataUnRouted(),
                stats.getDataEventInserted(), stats.getDataExtracted(),
                stats.getDataBytesExtracted(), stats.getDataExtractedErrors(),
                stats.getDataSent(), stats.getDataBytesSent(),
                stats.getDataSentErrors(), stats.getDataLoaded(),
                stats.getDataBytesLoaded(), stats.getDataLoadedErrors() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT });
    }
}