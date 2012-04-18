package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class PurgeServiceSqlMap extends AbstractSqlMap {

    public PurgeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);
        
        // @formatter:off

        putSql("selectOutgoingBatchRangeSql" ,"" + 
"select min(batch_id) as min_id, max(batch_id) as max_id from $(outgoing_batch) where                                           " + 
"  create_time < ? and status in ('OK','IG') and batch_id < (select max(batch_id) from $(outgoing_batch))   " );

        putSql("deleteOutgoingBatchSql" ,"" + 
"delete from $(outgoing_batch) where status in ('OK','IG') and batch_id between ?           \n" + 
"  and ? and batch_id not in (select batch_id from $(data_event) where batch_id between ?   \n" + 
"  and ?)                                                                                   \n" );

        putSql("deleteDataEventSql" ,"" + 
"delete from $(data_event) where batch_id not in (select batch_id from               \n" + 
"  $(outgoing_batch) where batch_id between ? and ? and status not in ('OK','IG'))   \n" + 
"  and batch_id between ? and ?                                                      \n" );

        putSql("deleteUnroutedDataEventSql" ,"" + 
"delete from $(data_event) where           " + 
"  batch_id=-1 and create_time  < ?                   " );

        putSql("selectDataRangeSql" ,"" + 
"select min(data_id) as min_id, max(data_id) as max_id from $(data) where data_id < (select max(data_id) from $(data))   " );

        putSql("updateStrandedBatches" ,"" + 
"update $(outgoing_batch) set status='OK' where node_id not                   " + 
"  in (select node_id from $(node) where sync_enabled=1) and status != 'OK'   " );

        putSql("deleteStrandedData" ,"" + 
"delete from $(data) where                                       \n" + 
"  data_id between ? and ? and                                   \n" + 
"  data_id < (select min(start_id) from sym_data_gap) and      \n" + 
"  create_time < ? and                                           \n" + 
"  data_id not in (select e.data_id from $(data_event) e where   \n" + 
"  e.data_id between ? and ?)                                    \n" );

        putSql("deleteDataSql" ,"" + 
"delete from $(data) where                                       \n" + 
"  data_id between ? and ? and                                   \n" + 
"  create_time < ? and                                           \n" + 
"  data_id in (select e.data_id from $(data_event) e where       \n" + 
"  e.data_id between ? and ?)                                    \n" + 
"  and                                                           \n" + 
"  data_id not in                                                \n" + 
"  (select e.data_id from $(data_event) e where                  \n" + 
"  e.data_id between ? and ? and                                 \n" + 
"  (e.data_id is null or                                         \n" + 
"  e.batch_id in                                                 \n" + 
"  (select batch_id from $(outgoing_batch) where                 \n" + 
"  status not in ('OK','IG'))))                                  \n" );

        putSql("selectIncomingBatchRangeSql" ,"" + 
"select node_id, min(batch_id) as min_id, max(batch_id) as max_id from $(incoming_batch) where   " + 
"  create_time < ? and status = 'OK' group by node_id                              " );

        putSql("deleteIncomingBatchSql" ,"" + 
"delete from $(incoming_batch) where batch_id between ? and ? and node_id =   " + 
"  ? and status = 'OK'                                                                   " );

        putSql("deleteFromDataGapsSql" ,"" + 
"delete from $(data_gap) where last_update_time < ? and status != 'GP'   " );

        putSql("deleteIncomingBatchByNodeSql" ,"" + 
"delete from $(incoming_batch) where node_id = ?   " );

    }

}