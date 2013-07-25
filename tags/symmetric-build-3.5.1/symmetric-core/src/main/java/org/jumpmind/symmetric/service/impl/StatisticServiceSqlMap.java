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

public class StatisticServiceSqlMap extends AbstractSqlMap {

    public StatisticServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("insertChannelStatsSql" ,"" + 
"insert into $(node_host_channel_stats)                     " + 
"  (node_id, host_name, channel_id, start_time, end_time,         " + 
"  data_routed, data_unrouted, data_event_inserted,               " + 
"  data_extracted, data_bytes_extracted, data_extracted_errors,   " + 
"  data_sent, data_bytes_sent, data_sent_errors,                  " + 
"  data_loaded, data_bytes_loaded, data_loaded_errors)            " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)                      " );

        putSql("selectChannelStatsSql" ,"" + 
"select node_id, host_name, channel_id, start_time, end_time,                       " + 
"  data_routed, data_unrouted, data_event_inserted,                                 " + 
"  data_extracted, data_bytes_extracted, data_extracted_errors,                     " + 
"  data_sent, data_bytes_sent, data_sent_errors,                                    " + 
"  data_loaded, data_bytes_loaded, data_loaded_errors                               " + 
"  from $(node_host_channel_stats)                                            " + 
"  where  start_time >= ? and end_time <= ? and node_id=? order by start_time asc   " );

        putSql("insertHostStatsSql" ,"" + 
"insert into $(node_host_stats)                                              " + 
"  (node_id, host_name, start_time, end_time,                                      " + 
"  restarted,nodes_pulled,nodes_pushed,nodes_rejected,                             " + 
"  nodes_registered,nodes_loaded,nodes_disabled,purged_data_rows,                  " + 
"  purged_data_event_rows,purged_batch_outgoing_rows,purged_batch_incoming_rows,   " + 
"  triggers_created_count,triggers_rebuilt_count,triggers_removed_count,           " + 
"  total_nodes_pull_time, total_nodes_push_time                                    " + 
"  )                                                                               " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)                                 " );

        putSql("selectHostStatsSql" ,"" + 
"select node_id, host_name, start_time, end_time,                                   " + 
"  restarted,nodes_pulled,nodes_pushed,nodes_rejected,                              " + 
"  nodes_registered,nodes_loaded,nodes_disabled,purged_data_rows,                   " + 
"  purged_data_event_rows,purged_batch_outgoing_rows,purged_batch_incoming_rows,    " + 
"  triggers_created_count,triggers_rebuilt_count,triggers_removed_count,            " + 
"  total_nodes_pull_time, total_nodes_push_time                                     " + 
"  from $(node_host_stats)                                                    " + 
"  where  start_time >= ? and end_time <= ? and node_id=? order by start_time asc   " );

        putSql("insertJobStatsSql" ,"" + 
"insert into $(node_host_job_stats)                 " + 
"  (node_id, host_name, job_name, start_time, end_time,   " + 
"  processed_count)                                       " + 
"  values(?,?,?,?,?,?)                                    " );

        putSql("selectJobStatsSql" ,"" + 
"select node_id, host_name, job_name, start_time, end_time,                         " + 
"  processed_count                                                                  " + 
"  from $(node_host_job_stats)                                                " + 
"  where  start_time >= ? and end_time <= ? and node_id=? order by start_time asc   " );

    }

}