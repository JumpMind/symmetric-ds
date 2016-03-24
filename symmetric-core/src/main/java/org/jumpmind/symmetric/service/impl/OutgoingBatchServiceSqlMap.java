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

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class OutgoingBatchServiceSqlMap extends AbstractSqlMap {

    public OutgoingBatchServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        
        putSql("selectNodesInErrorSql", "select distinct node_id from $(outgoing_batch) where error_flag=1");

        putSql("initialLoadStatusSql", ""
                + "select status from $(outgoing_batch) where node_id=? and load_flag=?   ");

        putSql("unsentBatchesForNodeIdChannelIdSql",
                ""
                        + "select count(*) from $(outgoing_batch) where node_id=? and channel_id=? and status != 'OK'   ");

        putSql("selectCountBatchesPrefixSql", "" + "select count(*) from $(outgoing_batch)   ");

        putSql("cancelLoadBatchesSql",
                "update $(outgoing_batch) set ignore_count=1, status='OK', error_flag=0 where load_id=?");

        putSql("insertOutgoingBatchSql",
                ""
                        + "insert into $(outgoing_batch)                                                                                                                "
                        + "  (batch_id, node_id, channel_id, status, load_id, extract_job_flag, load_flag, common_flag, reload_event_count, other_event_count, last_update_hostname, last_update_time, create_time, create_by)   "
                        + "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, ?)                                                                         ");

        putSql("updateOutgoingBatchSql",
                ""
                        + "update $(outgoing_batch) set status=?, load_id=?, extract_job_flag=?, load_flag=?, error_flag=?,                                          "
                        + "  byte_count=?, extract_count=?, sent_count=?, load_count=?, data_event_count=?,                                 "
                        + "  reload_event_count=?, insert_event_count=?, update_event_count=?, delete_event_count=?, other_event_count=?,   "
                        + "  ignore_count=?, router_millis=?, network_millis=?, filter_millis=?,                                                            "
                        + "  load_millis=?, extract_millis=?, sql_state=?, sql_code=?, sql_message=?,                                       "
                        + "  failed_data_id=?, last_update_hostname=?, last_update_time=? where batch_id=? and node_id=?                    ");

        putSql("findOutgoingBatchSql", "where batch_id=? and node_id=?  ");

        putSql("findOutgoingBatchByIdOnlySql", "where batch_id=? ");

        putSql("selectOutgoingBatchSql", ""
                + "where node_id = ? and status in (?, ?, ?, ?, ?, ?, ?) order by batch_id asc   ");

        putSql("selectOutgoingBatchRangeSql", ""
                + "where batch_id between ? and ? order by batch_id   ");

        putSql("selectOutgoingBatchTimeRangeSql", ""
                + "where node_id=? and channel_id=? and create_time >= ? and create_time <= ? ");

        putSql("selectOutgoingBatchPrefixSql",
                ""
                        + "select b.node_id, b.channel_id, b.status,                                                                              "
                        + "  b.byte_count, b.extract_count, b.sent_count, b.load_count, b.data_event_count,                                           "
                        + "  b.reload_event_count, b.insert_event_count, b.update_event_count, b.delete_event_count, b.other_event_count,             "
                        + "  b.ignore_count, b.router_millis, b.network_millis, b.filter_millis, b.load_millis, b.extract_millis, b.sql_state, b.sql_code,  "
                        + "  b.sql_message,   "
                        + "  b.failed_data_id, b.last_update_hostname, b.last_update_time, b.create_time, b.batch_id, b.extract_job_flag, b.load_flag, b.error_flag, b.common_flag, b.load_id, b.create_by from      "
                        + "  $(outgoing_batch) b                                                                                       ");

        putSql("selectOutgoingBatchErrorsSql", " where error_flag=1 order by batch_id   ");

        putSql("countOutgoingBatchesErrorsOnChannelSql", ""
                + "select count(*) from $(outgoing_batch) where error_flag=1 and channel_id=?");

        putSql("countOutgoingBatchesErrorsSql", ""
                + "select count(*) from $(outgoing_batch) where error_flag=1");

        putSql("countOutgoingBatchesUnsentSql", ""
                + "select count(*) from $(outgoing_batch) where status != 'OK'");
        
        putSql("countOutgoingBatchesWithStatusSql", ""
                + "select count(*) from $(outgoing_batch) where status = ? ");

        putSql("countOutgoingBatchesUnsentOnChannelSql", ""
                + "select count(*) from $(outgoing_batch) where status != 'OK' and channel_id=?");

        putSql("selectOutgoingBatchSummaryByStatusSql",
                "select count(*) as batches, sum(data_event_count) as data, status, node_id, min(create_time) as oldest_batch_time       "
                        + "  from $(outgoing_batch) where status in (:STATUS_LIST) group by status, node_id order by oldest_batch_time asc   ");

        putSql("updateOutgoingBatchesStatusSql", ""
                + "update $(outgoing_batch) set status=? where status = ?   ");

        putSql("getLoadSummariesSql",
                "select b.load_id, b.node_id, b.status, b.create_by, max(error_flag) as error_flag, count(*) as cnt, min(b.create_time) as create_time,          "
              + "       max(b.last_update_time) as last_update_time, min(b.batch_id) as current_batch_id,  "
              + "       min(b.data_event_count) as current_data_event_count                                                                                      "
              + "from $(outgoing_batch) b inner join                                                                                                            "
              + "     $(data_event) e on b.batch_id=e.batch_id inner join                                                                                       "
              + "     $(data) d on d.data_id=e.data_id                                                                                                          "
              + "where b.channel_id='reload'                                                                                                                     "
              + "group by b.load_id, b.node_id, b.status, b.create_by                                                                              "
              + "order by b.load_id desc                                                                                                                         ");

        putSql("getNextOutgoingBatchForEachNodeSql",
                "select min(b.batch_id) as batch_id, b.node_id, b.status, b.channel_id        "
              + "  from $(outgoing_batch) b where status != 'OK' and status != 'RT'          "
              + "  group by b.node_id, b.status, b.channel_id");

        putSql("deleteOutgoingBatchesForNodeSql", 
                "delete from $(outgoing_batch) where node_id=? and channel_id=? and batch_id < "
                + "(select max(batch_id) from $(outgoing_batch) where node_id=? and channel_id=?) ");

        putSql("copyOutgoingBatchesSql", 
                  "insert into $(outgoing_batch)                                                                                                                 "
                + "  (batch_id, node_id, channel_id, status, load_id, extract_job_flag, load_flag, common_flag, reload_event_count, other_event_count,           "
                + "  last_update_hostname, last_update_time, create_time, create_by)                                                                             "
                + "  (select batch_id, ?, channel_id, 'NE', load_id, extract_job_flag, load_flag, common_flag, reload_event_count, other_event_count,          " 
                + "   last_update_hostname, current_timestamp, create_time, 'copy' from $(outgoing_batch) where node_id=? and channel_id=? and batch_id > ?)     ");


    }

}