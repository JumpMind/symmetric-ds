package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class OutgoingBatchServiceSqlMap extends AbstractSqlMap {

    public OutgoingBatchServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("initialLoadStatusSql" ,"" + 
"select status from $(outgoing_batch) where node_id=? and load_flag=?   " );

        putSql("unsentBatchesForNodeIdChannelIdSql" ,"" + 
"select count(*) from $(outgoing_batch) where node_id=? and channel_id=? and status != 'OK'   " );

        putSql("selectCountBatchesPrefixSql" ,"" + 
"select count(*) from $(outgoing_batch)   " );

        putSql("insertOutgoingBatchSql" ,"" + 
"insert into $(outgoing_batch)                                                                                                                " + 
"  (batch_id, node_id, channel_id, status, load_flag, common_flag, reload_event_count, other_event_count, last_update_hostname, last_update_time, create_time)   " + 
"  values (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)                                                                         " );

        putSql("updateOutgoingBatchSql" ,"" + 
"update $(outgoing_batch) set status=?, load_flag=?, error_flag=?,                                          " + 
"  byte_count=?, extract_count=?, sent_count=?, load_count=?, data_event_count=?,                                 " + 
"  reload_event_count=?, insert_event_count=?, update_event_count=?, delete_event_count=?, other_event_count=?,   " + 
"  ignore_count=?, router_millis=?, network_millis=?, filter_millis=?,                                                            " + 
"  load_millis=?, extract_millis=?, sql_state=?, sql_code=?, sql_message=?,                                       " + 
"  failed_data_id=?, last_update_hostname=?, last_update_time=? where batch_id=? and node_id=?                    " );

        putSql("findOutgoingBatchSql" ,"" + 
"where batch_id=? and node_id=?  " );

        putSql("selectOutgoingBatchSql" ,"" + 
"where node_id = ? and status in (?, ?, ?, ?, ?, ?) order by batch_id asc   " );

        putSql("selectOutgoingBatchRangeSql" ,"" + 
"where batch_id between ? and ? order by batch_id   " );

        putSql("selectOutgoingBatchPrefixSql" ,"" + 
"select node_id, channel_id, status,                                                                              " + 
"  byte_count, extract_count, sent_count, load_count, data_event_count,                                           " + 
"  reload_event_count, insert_event_count, update_event_count, delete_event_count, other_event_count,             " + 
"  ignore_count, router_millis, network_millis, filter_millis, load_millis, extract_millis, sql_state, sql_code,  " +
"  sql_message,   " + 
"  failed_data_id, last_update_hostname, last_update_time, create_time, batch_id, load_flag, error_flag, common_flag from      " + 
"  $(outgoing_batch)                                                                                        " );

        putSql("selectOutgoingBatchErrorsSql" , 
" where error_flag=1 order by batch_id   " );

        putSql("countOutgoingBatchesErrorsOnChannelSql" ,"" + 
"select count(*) from $(outgoing_batch) where error_flag=1 and channel_id=?" );
        
        putSql("countOutgoingBatchesUnsentSql" ,"" + 
"select count(*) from $(outgoing_batch) where status != 'OK'" );
        
        putSql("countOutgoingBatchesUnsentOnChannelSql" ,"" + 
"select count(*) from $(outgoing_batch) where status != 'OK' and channel_id=?" );        

        putSql("selectOutgoingBatchSummaryByStatusSql" ,"" + 
"select count(*) as batches, sum(data_event_count) as data, status, node_id, min(create_time) as oldest_batch_time       " + 
"  from $(outgoing_batch) where status in (:STATUS_LIST) group by status, node_id order by oldest_batch_time asc   " );

        putSql("updateOutgoingBatchesStatusSql" ,"" + 
"update $(outgoing_batch) set status=? where status = ?   " );

    }

}