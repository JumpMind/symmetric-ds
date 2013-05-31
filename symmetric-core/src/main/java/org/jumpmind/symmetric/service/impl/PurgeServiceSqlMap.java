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

public class PurgeServiceSqlMap extends AbstractSqlMap {

    public PurgeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);
        
        // @formatter:off

        putSql("selectOutgoingBatchRangeSql" ,
"select min(batch_id) as min_id, max(batch_id) as max_id from $(outgoing_batch) where                                           " + 
"  create_time < ? and status = ? and batch_id < (select max(batch_id) from $(outgoing_batch))   " );

        putSql("deleteOutgoingBatchSql" ,
"delete from $(outgoing_batch) where status = ? and batch_id between ?                " + 
"  and ? and batch_id not in (select batch_id from $(data_event) where batch_id between ?   " + 
"  and ?)                                                                                   " );

        putSql("deleteDataEventSql" ,
"delete from $(data_event) where batch_id not in (select batch_id from               " + 
"  $(outgoing_batch) where batch_id between ? and ? and status != ?)        " + 
"  and batch_id between ? and ?                                                      " );

        putSql("selectDataRangeSql" ,
"select min(data_id) as min_id, max(data_id) as max_id from $(data) where data_id < (select max(data_id) from $(data))   " );

        putSql("updateStrandedBatches" ,
"update $(outgoing_batch) set status=? where node_id not                   " + 
"  in (select node_id from $(node) where sync_enabled=?) and status != ?   " );

        putSql("deleteStrandedData" ,
"delete from $(data) where                                       " + 
"  data_id between ? and ? and                                   " + 
"  data_id < (select min(start_id) from $(data_gap)) and      " + 
"  create_time < ? and                                           " + 
"  data_id not in (select e.data_id from $(data_event) e where   " + 
"  e.data_id between ? and ?)                                    " );

        putSql("deleteDataSql" ,
"delete from $(data) where                                       " + 
"  data_id between ? and ? and                                   " + 
"  create_time < ? and                                           " + 
"  data_id in (select e.data_id from $(data_event) e where       " + 
"  e.data_id between ? and ?)                                    " + 
"  and                                                           " + 
"  data_id not in                                                " + 
"  (select e.data_id from $(data_event) e where                  " + 
"  e.data_id between ? and ? and                                 " + 
"  (e.data_id is null or                                         " + 
"  e.batch_id in                                                 " + 
"  (select batch_id from $(outgoing_batch) where                 " + 
"  status != ?)))                                  " );

        putSql("selectIncomingBatchRangeSql" ,
"select node_id, min(batch_id) as min_id, max(batch_id) as max_id from $(incoming_batch) where   " + 
"  create_time < ? and status = ? group by node_id                              " );

        putSql("deleteIncomingBatchSql" ,
"delete from $(incoming_batch) where batch_id between ? and ? and node_id = ? and status = ?" );
        
        putSql("deleteIncomingErrorsSql", "delete from $(incoming_error) where batch_id not in (select batch_id from $(incoming_batch))");

        putSql("deleteFromDataGapsSql" ,
"delete from $(data_gap) where last_update_time < ? and status != ?" );

        putSql("deleteIncomingBatchByNodeSql" ,
"delete from $(incoming_batch) where node_id = ?   " );
        
        putSql("purgeNodeHostChannelStatsSql", "delete from $(node_host_channel_stats) where start_time < ?");
        
        putSql("purgeNodeHostStatsSql", "delete from $(node_host_stats) where start_time < ?");
                
        putSql("purgeNodeHostJobStatsSql", "delete from $(node_host_job_stats) where start_time < ?");

    }

}