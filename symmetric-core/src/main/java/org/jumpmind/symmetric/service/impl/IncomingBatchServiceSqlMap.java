package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class IncomingBatchServiceSqlMap extends AbstractSqlMap {

    public IncomingBatchServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);
        
        // @formatter:off

        putSql("selectIncomingBatchPrefixSql" ,"" + 
"select batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count,           " + 
"  statement_count, fallback_insert_count, fallback_update_count, ignore_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message,   " + 
"  last_update_hostname, last_update_time, create_time, error_flag from $(incoming_batch)                                         " );

        putSql("selectCreateTimePrefixSql" ,"" + 
"select create_time from $(incoming_batch)   " );

        putSql("findIncomingBatchSql" ,"" + 
"where batch_id = ? and node_id = ?   " );

        putSql("listIncomingBatchesSql" ,"" + 
"where node_id in (:NODES) and channel_id in (:CHANNELS) and status in (:STATUSES) " );

        putSql("listIncomingBatchesInErrorSql" ,"" + 
"where node_id in (:NODES) and channel_id in (:CHANNELS) and error_flag=1 " );

        putSql("listIncomingBatchesInErrorForNodeSql" ,"" + 
"where node_id=? and error_flag=1   " );

        putSql("findIncomingBatchErrorsSql" ,"" + 
"where status = 'ER' order by batch_id   " );

        putSql("countIncomingBatchesErrorsSql" ,"" + 
"select count(*) from $(incoming_batch) where error_flag=1   " );

        putSql("insertIncomingBatchSql" ,"" + 
"insert into $(incoming_batch) (batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count,   " + 
"  statement_count, fallback_insert_count, fallback_update_count, ignore_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message,                         " + 
"  last_update_hostname, last_update_time, create_time)                                                                                                       " + 
"  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)                                                                        " );

        putSql("updateIncomingBatchSql" ,"" + 
"update $(incoming_batch) set status = ?, error_flag=?, network_millis = ?, filter_millis = ?, database_millis = ?, failed_row_number = ?, failed_line_number = ?, byte_count = ?,         " + 
"  statement_count = ?, fallback_insert_count = ?, fallback_update_count = ?, ignore_count = ?, missing_delete_count = ?, skip_count = ?,  sql_state = ?, sql_code = ?, sql_message = ?,   " + 
"  last_update_hostname = ?, last_update_time = ? where batch_id = ? and node_id = ?                                                                                     " );

    }

}