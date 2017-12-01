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

        putSql("initialLoadStatusSql",
                "select status from $(outgoing_batch) where node_id=? and load_flag=?   ");

        putSql("unsentBatchesForNodeIdChannelIdSql",
                "select count(*) from $(outgoing_batch) where node_id=? and channel_id=? and status != 'OK'   ");

        putSql("selectCountBatchesPrefixSql", "select count(*) from $(outgoing_batch)   ");

        putSql("cancelLoadBatchesSql",
                "update $(outgoing_batch) set ignore_count=1, status='OK', error_flag=0, last_update_time=current_timestamp where load_id=?");

        putSql("cancelChannelBatchesSelectSql",
                "select node_id, batch_id from $(outgoing_batch) where channel_id = ? and status <> ?");
        
        putSql("cancelChannelBatchesSelectTableSql", " and summary=?");
        
        putSql("cancelChannelBatchSql",
                "update $(outgoing_batch) set ignore_count=1, status='OK', error_flag=0, last_update_time=current_timestamp where node_id = ? and batch_id = ?");

        putSql("insertOutgoingBatchSql",
                        "insert into $(outgoing_batch)                                                                                                                "
                        + "  (batch_id, node_id, channel_id, status, load_id, extract_job_flag, load_flag, common_flag, reload_event_count, other_event_count, last_update_hostname, last_update_time, create_time, create_by, summary)   "
                        + "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, ?, ?)                                                                         ");

        putSql("updateOutgoingBatchSql",
                        "update $(outgoing_batch) set status=?, load_id=?, extract_job_flag=?, load_flag=?, error_flag=?,                                          "
                        + "  byte_count=?, extract_count=?, sent_count=?, load_count=?, data_event_count=?,                                 "
                        + "  reload_event_count=?, insert_event_count=?, update_event_count=?, delete_event_count=?, other_event_count=?,   "
                        + "  ignore_count=?, router_millis=?, network_millis=?, filter_millis=?,                                                            "
                        + "  load_millis=?, extract_millis=?, sql_state=?, sql_code=?, sql_message=?,                                       "
                        + "  failed_data_id=?, last_update_hostname=?, last_update_time=current_timestamp, summary=? where batch_id=? and node_id=?                    ");

        putSql("findOutgoingBatchSql", "where batch_id=? and node_id=?  ");

        putSql("findOutgoingBatchByIdOnlySql", "where batch_id=? ");

        putSql("selectOutgoingBatchSql",
                "where node_id = ? and status in (?, ?, ?, ?, ?, ?, ?, ?) order by batch_id asc   ");

        putSql("selectOutgoingBatchChannelSql", 
                " join $(channel) c on c.channel_id = b.channel_id where node_id = ? and c.queue = ? and status in (?, ?, ?, ?, ?, ?, ?, ?) order by batch_id asc   ");

        putSql("selectOutgoingBatchChannelActionSql", 
                " join $(channel) c on c.channel_id = b.channel_id" + 
                " where c.data_event_action = ?" +
                " and b.node_id = ? and c.queue = ? and b.status in (?, ?, ?, ?, ?, ?, ?, ?) order by b.batch_id asc   ");

        putSql("selectOutgoingBatchChannelActionNullSql", 
                " join $(channel) c on c.channel_id = b.channel_id" + 
                " where (c.data_event_action is null or c.data_event_action = ?)" +
                " and b.node_id = ? and c.queue = ? and b.status in (?, ?, ?, ?, ?, ?, ?, ?) order by b.batch_id asc   ");

        putSql("selectOutgoingBatchRangeSql",
                "where batch_id between ? and ? order by batch_id   ");

        putSql("selectOutgoingBatchLoadSql",
                "where load_id = ? order by batch_id   ");

        putSql("selectOutgoingBatchTimeRangeSql",
                "where node_id=? and channel_id=? and create_time >= ? and create_time <= ? ");

        putSql("selectOutgoingBatchPrefixSql",
                        "select b.node_id, b.channel_id, b.status,                                                                              "
                        + "  b.byte_count, b.extract_count, b.sent_count, b.load_count, b.data_event_count,                                           "
                        + "  b.reload_event_count, b.insert_event_count, b.update_event_count, b.delete_event_count, b.other_event_count,             "
                        + "  b.ignore_count, b.router_millis, b.network_millis, b.filter_millis, b.load_millis, b.extract_millis, b.sql_state, b.sql_code,  "
                        + "  b.sql_message,   "
                        + "  b.failed_data_id, b.last_update_hostname, b.last_update_time, b.create_time, b.batch_id, b.extract_job_flag, b.load_flag, b.error_flag, b.common_flag, b.load_id, b.create_by, b.summary from      "
                        + "  $(outgoing_batch) b                                                                                       ");

        putSql("selectOutgoingBatchErrorsSql", " where error_flag=1 order by batch_id   ");

        putSql("countOutgoingBatchesErrorsOnChannelSql", 
                "select count(*) from $(outgoing_batch) where error_flag=1 and channel_id=?");
        
        putSql("countOutgoingBatchesByChannelSql", 
                "select count(*) as batch_count, channel_id from $(outgoing_batch) where node_id = ? and channel_id <> 'heartbeat' and status in ('ER','RQ','NE','QY','RT') group by channel_id order by batch_count desc, channel_id");

        putSql("countOutgoingBatchesErrorsSql",
                "select count(*) from $(outgoing_batch) where error_flag=1");

        putSql("countOutgoingBatchesUnsentSql",
                "select count(*) from $(outgoing_batch) where status != 'OK'");
        
        putSql("countOutgoingBatchesWithStatusSql",
                "select count(*) from $(outgoing_batch) where status = ? ");

        putSql("countOutgoingBatchesUnsentOnChannelSql",
                "select count(*) from $(outgoing_batch) where status != 'OK' and channel_id=?");

        putSql("selectOutgoingBatchSummaryByStatusSql",
                "select count(*) as batches, sum(data_event_count) as data, status, node_id, min(create_time) as oldest_batch_time       "
                        + "  from $(outgoing_batch) where status in (:STATUS_LIST) group by status, node_id order by oldest_batch_time asc   ");

        putSql("updateOutgoingBatchesStatusSql",
                "update $(outgoing_batch) set status=?, last_update_time=current_timestamp where status = ?   ");

        putSql("getLoadSummariesSql",
                "select b.load_id, b.node_id, b.status, b.create_by, max(error_flag) as error_flag, count(*) as cnt, min(b.create_time) as create_time,          "
              + "       max(b.last_update_time) as last_update_time, min(b.batch_id) as current_batch_id,  "
              + "       min(b.data_event_count) as current_data_event_count, b.channel_id                                                                                      "
              + "from $(outgoing_batch) b                                                                                                   "
              + "     join $(channel) c on c.channel_id = b.channel_id 																							"					
              + "where c.reload_flag = 1                                                                                                                    "
              + " and b.load_id > 0                      				                                                                                    "
              + "group by b.load_id, b.node_id, b.status, b.channel_id, b.create_by                                                                              "
              + "order by b.load_id desc                                                                                                                         ");

        putSql("getActiveLoadsSql", 
                  "select r.load_id "
                + "from $(table_reload_request) r "
                + "join $(outgoing_batch) ob on ob.load_id = r.load_id " 
                + "where ob.status != 'OK' and ob.status != 'IG' and r.source_node_id = ? "
                + "group by r.load_id");
        
        putSql("getUnprocessedReloadRequestsSql", 
                "select r.source_node_id, r.target_node_id "
              + "from $(table_reload_request) r "
              + "where processed = 0 and source_node_id = ? "
        	  + "group by r.source_node_id, r.target_node_id");
      
        putSql("getLoadSummarySql",
                "select " 
                + "   target_node_id, load_id, count(TRIGGER_ID) as table_count, max(TRIGGER_ID) as trigger_id, "
                + "   max(create_table) as create_table, max(delete_first) as delete_first, max(processed) as processed, " 
                + "   max(reload_select) as reload_select, max(before_custom_sql) as before_custom_sql, " 
                + "   max(last_update_by) as last_update_by, min(last_update_time) as last_update_time "
                + "from $(table_reload_request) "
                + "    where load_id = ? "
                + "    group by load_id, target_node_id ");
             
        putSql("getLoadStatusSummarySql", 
                "select count(ob.batch_id) as count, ob.status, c.queue, max(ob.last_update_time) as last_update_time, "
                + " min(ob.create_time) as create_time, sum(ob.data_event_count) as data_events, sum(ob.byte_count) as byte_count"
                + "   from $(outgoing_batch) ob "
                + "   join $(channel) c on c.channel_id = ob.channel_id "
                + "   where ob.load_id = ? "
                + "   group by c.queue, ob.status");
        
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

        
        putSql("getAllBatchesSql", "select batch_id from $(outgoing_batch)");

    }

}