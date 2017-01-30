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

public class IncomingBatchServiceSqlMap extends AbstractSqlMap {

    public IncomingBatchServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);
        
        // @formatter:off
        
        putSql("selectNodesInErrorSql", "select distinct node_id from $(incoming_batch) where error_flag=1");

        putSql("selectIncomingBatchPrefixSql" ,"" + 
"select batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count,           " + 
"  statement_count, fallback_insert_count, fallback_update_count, ignore_count, ignore_row_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message,   " + 
"  last_update_hostname, last_update_time, create_time, error_flag, summary from $(incoming_batch)                                         " );

        putSql("selectCreateTimePrefixSql" ,"" + 
"select create_time from $(incoming_batch)   " );

        putSql("findIncomingBatchSql" ,"" + 
"where batch_id = ? and node_id = ?   " );

        putSql("findIncomingBatchByBatchIdSql", "where batch_id = ? " );

        putSql("listIncomingBatchesInErrorForNodeSql" ,"" + 
"where node_id=? and error_flag=1   " );

        putSql("findIncomingBatchErrorsSql" ,"" + 
"where status = 'ER' order by batch_id   " );

        putSql("countIncomingBatchesErrorsSql" ,"" + 
"select count(*) from $(incoming_batch) where error_flag=1   " );

        putSql("countIncomingBatchesErrorsOnChannelSql" ,"" + 
"select count(*) from $(incoming_batch) where error_flag=1 and channel_id=?");
        
        putSql("insertIncomingBatchSql" ,"" + 
"insert into $(incoming_batch) (batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count,   " + 
"  statement_count, fallback_insert_count, fallback_update_count, ignore_count, ignore_row_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message,                         " + 
"  last_update_hostname, last_update_time, summary, create_time)                                                                                                       " + 
"  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)                                                                        " );

        putSql("updateIncomingBatchSql" ,"" + 
"update $(incoming_batch) set status = ?, error_flag=?, network_millis = ?, filter_millis = ?, database_millis = ?, failed_row_number = ?, failed_line_number = ?, byte_count = ?,         " + 
"  statement_count = ?, fallback_insert_count = ?, fallback_update_count = ?, ignore_count = ?, ignore_row_count = ?, missing_delete_count = ?, skip_count = ?,  sql_state = ?, sql_code = ?, sql_message = ?,   " + 
"  last_update_hostname = ?, last_update_time = ?, summary = ? where batch_id = ? and node_id = ?                                                                                     " );

        putSql("deleteIncomingBatchSql" ,"" + 
"delete from $(incoming_batch) where batch_id = ? and node_id = ?                                                                                     " );

        putSql("deleteIncomingBatchByNodeSql" ,"delete from $(incoming_batch) where node_id = ?");
        
        putSql("maxBatchIdsSql", "select max(batch_id) as batch_id, node_id, channel_id from $(incoming_batch) where status = ? group by node_id, channel_id");
        
        putSql("selectIncomingBatchSummaryByStatusAndChannelSql",
                "select count(*) as batches, status, sum(statement_count) as data, node_id, min(create_time) as oldest_batch_time, channel_id,      "
                        + " max(last_update_time) as last_update_time, max(cast(sql_message as varchar)) as sql_message, min(batch_id) as batch_id "
                        + "  from $(incoming_batch) where status in (:STATUS_LIST) group by status, node_id, channel_id order by oldest_batch_time asc   ");

        putSql("selectIncomingBatchSummaryByStatusSql",
                "select count(*) as batches, status, sum(statement_count) as data, node_id, min(create_time) as oldest_batch_time,      "
                        + " max(last_update_time) as last_update_time"
                        + "  from $(incoming_batch) where status in (:STATUS_LIST) group by status, node_id order by oldest_batch_time asc   ");

        putSql("lastUpdateByChannelSql", "select max(last_update_time) as last_update_time, channel_id from $(incoming_batch) group by channel_id");
        
        putSql("getAllBatchesSql", "select batch_id, node_id from $(incoming_batch)");
    
    }

}