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
        
        putSql("minDataGapStartId", "select min(start_id) from $(data_gap)");
        
        putSql("deleteExtractRequestSql", "delete from $(extract_request) where status=? and last_update_time < ? and "
                + "0 = (select count(1) from $(outgoing_batch) where status != 'OK' and batch_id between $(extract_request).start_batch_id and $(extract_request).end_batch_id)");
        
        putSql("deleteRegistrationRequestSql", "delete from $(registration_request) where status in (?,?,?) and last_update_time < ?");
        
        putSql("deleteMonitorEventSql", "delete from $(monitor_event) where event_time < ?");

        putSql("selectOutgoingBatchRangeSql" ,
"select min(batch_id) as min_id, max(batch_id) as max_id from $(outgoing_batch) where                         " + 
"  create_time < ? and status = ? and batch_id < (select max(batch_id) from $(outgoing_batch))                " );

        putSql("deleteOutgoingBatchSql" ,
"delete from $(outgoing_batch) where status = ? and batch_id between ?                " + 
"  and ? and batch_id not in (select batch_id from $(data_event) where batch_id between ?   " + 
"  and ?)                                                                                   " );

        putSql("deleteDataEventSql" ,
"delete from $(data_event) where batch_id not in (select batch_id from               " + 
"  $(outgoing_batch) where batch_id between ? and ? and status != ?)                 " + 
"  and batch_id between ? and ?                                                      " );

        putSql("selectDataRangeSql" ,
"select min(data_id) as min_id, max(data_id) as max_id from $(data) where data_id < (select max(data_id) from $(data))   " );

        putSql("selectNodesWithStrandedBatches", "select distinct node_id from $(outgoing_batch) " + 
"where node_id not in (select node_id from $(node) where sync_enabled = ?) and status != ?");

        putSql("updateStrandedBatches", "update $(outgoing_batch) set status=? where node_id=? and status != ?");

        putSql("selectChannelsWithStrandedBatches", "select distinct channel_id from $(outgoing_batch) " +
"where channel_id not in (select channel_id from $(channel)) and status != ?");

        putSql("updateStrandedBatchesByChannel", "update $(outgoing_batch) set status=? where channel_id=? and status != ?");

        putSql("deleteStrandedData" ,
"delete from $(data) where                                       " + 
"  data_id between ? and ? and                                   " + 
"  data_id < ? and         " + 
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
"  create_time < ? and status = ? group by node_id                                               " );

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
        
        putSql("selectIncomingErrorsBatchIdsSql", "select distinct e.batch_id as batch_id from $(incoming_error) e LEFT OUTER JOIN $(incoming_batch) i ON e.batch_id = i.batch_id where i.batch_id IS NULL");
        
        putSql("deleteIncomingErrorsBatchIdsSql", "delete from $(incoming_error) where batch_id IN (?)");
        
        putSql("deleteOutgoingBatchByCreateTimeSql", "delete from $(outgoing_batch) where create_time < ?");
        putSql("deleteDataEventByCreateTimeSql", "delete from $(data_event) where create_time < ?");
        putSql("deleteDataByCreateTimeSql", "delete from $(data) where create_time < ?");
        putSql("deleteExtractRequestByCreateTimeSql", "delete from $(extract_request) where create_time < ?");

        putSql("selectStrandedDataEventRangeSql" ,
"select min(batch_id) as min_id, max(batch_id)+1 as max_id from $(data_event) " + 
"where create_time < ? " +
"and batch_id < (select min(batch_id) from $(outgoing_batch))");

        putSql("deleteStrandedDataEvent",
"delete from $(data_event) " + 
"where batch_id between ? and ? " +
"and create_time < ? " +
"and batch_id not in (select batch_id from $(outgoing_batch) where batch_id between ? and ?)");

        putSql("minOutgoingBatchNotStatusSql",
                "select min(batch_id) from $(outgoing_batch) where status != ?");

        putSql("deleteDataEventByRangeSql", "delete from $(data_event) where batch_id between ? and ?");

        putSql("deleteOutgoingBatchByRangeSql", "delete from $(outgoing_batch) where batch_id between ? and ?");

        putSql("countOutgoingBatchNotStatusSql",
                "select count(*) from $(outgoing_batch) where status != ?");

        putSql("selectDataEventMinNotStatusSql", "select min(data_id) from $(data_event) " +
                "where batch_id in (select batch_id from $(outgoing_batch) where status != ?)");

        putSql("deleteDataByRangeSql", "delete from $(data) where data_id between ? and ? and create_time < ?");

        putSql("selectOldChannelsForData", "select distinct channel_id from $(data) where channel_id not in (select channel_id from $(channel))");
        
        putSql("deleteDataByChannel", "delete from $(data) where channel_id = ?");

    }

}