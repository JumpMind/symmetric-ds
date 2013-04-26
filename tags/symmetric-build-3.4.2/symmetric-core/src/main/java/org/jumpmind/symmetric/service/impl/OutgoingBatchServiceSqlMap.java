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
"  (batch_id, node_id, channel_id, status, load_id, load_flag, common_flag, reload_event_count, other_event_count, last_update_hostname, last_update_time, create_time, create_by)   " + 
"  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, ?)                                                                         " );

        putSql("updateOutgoingBatchSql" ,"" + 
"update $(outgoing_batch) set status=?, load_id=?, load_flag=?, error_flag=?,                                          " + 
"  byte_count=?, extract_count=?, sent_count=?, load_count=?, data_event_count=?,                                 " + 
"  reload_event_count=?, insert_event_count=?, update_event_count=?, delete_event_count=?, other_event_count=?,   " + 
"  ignore_count=?, router_millis=?, network_millis=?, filter_millis=?,                                                            " + 
"  load_millis=?, extract_millis=?, sql_state=?, sql_code=?, sql_message=?,                                       " + 
"  failed_data_id=?, last_update_hostname=?, last_update_time=? where batch_id=? and node_id=?                    " );

        putSql("findOutgoingBatchSql" , "where batch_id=? and node_id=?  " );

        putSql("findOutgoingBatchByIdOnlySql" , "where batch_id=? " );        

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
"  failed_data_id, last_update_hostname, last_update_time, create_time, batch_id, load_flag, error_flag, common_flag, load_id, create_by from      " + 
"  $(outgoing_batch)                                                                                        " );

        putSql("selectOutgoingBatchErrorsSql" , 
" where error_flag=1 order by batch_id   " );

        putSql("countOutgoingBatchesErrorsOnChannelSql" ,"" + 
"select count(*) from $(outgoing_batch) where error_flag=1 and channel_id=?" );
        
        putSql("countOutgoingBatchesErrorsSql" ,"" + 
"select count(*) from $(outgoing_batch) where error_flag=1" );
        
        
        putSql("countOutgoingBatchesUnsentSql" ,"" + 
"select count(*) from $(outgoing_batch) where status != 'OK'" );
        
        putSql("countOutgoingBatchesUnsentOnChannelSql" ,"" + 
"select count(*) from $(outgoing_batch) where status != 'OK' and channel_id=?" );        

        putSql("selectOutgoingBatchSummaryByStatusSql" ,"" + 
"select count(*) as batches, sum(data_event_count) as data, status, node_id, min(create_time) as oldest_batch_time       " + 
"  from $(outgoing_batch) where status in (:STATUS_LIST) group by status, node_id order by oldest_batch_time asc   " );

        putSql("updateOutgoingBatchesStatusSql" ,"" + 
"update $(outgoing_batch) set status=? where status = ?   " );
        
        putSql("getLoadSummariesSql", 
                "select b.load_id, b.node_id, b.status, b.create_by, max(error_flag) as error_flag, count(*) as count, min(b.create_time) as create_time, " +
        		"       max(b.last_update_time) as last_update_time, d.event_type, min(b.batch_id) as current_batch_id, min(d.table_name) as current_table_name, " +
        		"       min(b.data_event_count) as current_data_event_count " +
        		"from sym_outgoing_batch b inner join " +
        		"     sym_data_event e on b.batch_id=e.batch_id inner join " +
        		"     sym_data d on d.data_id=e.data_id " +
        		"where b.channel_id='reload' " +
        		"group by b.load_id, b.node_id, b.status, d.event_type, b.create_by " +
        		"order by b.load_id desc");

    }

}