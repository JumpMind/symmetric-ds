package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class PurgeServiceSqlMap extends AbstractSqlMap {

    public PurgeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectOutgoingBatchRangeSql" ,"" + 
"select min(batch_id) as min_id, max(batch_id) as max_id from $(outgoing_batch) where                                           " + 
"  create_time < ? and status in ('OK','IG') and batch_id < (select max(batch_id) from $(outgoing_batch))   " );

        putSql("deleteOutgoingBatchSql" ,"" + 
"delete from $(outgoing_batch) where status in ('OK','IG') and batch_id between :MIN              " + 
"  and :MAX and batch_id not in (select batch_id from $(data_event) where batch_id between :MIN   " + 
"  and :MAX)                                                                                                 " );

        putSql("deleteDataEventSql" ,"" + 
"delete from $(data_event) where batch_id not in (select batch_id from                     " + 
"  $(outgoing_batch) where batch_id between :MIN and :MAX and status not in ('OK','IG'))   " + 
"  and batch_id between :MIN and :MAX                                                                 " );

        putSql("deleteUnroutedDataEventSql" ,"" + 
"delete from $(data_event) where           " + 
"  batch_id=-1 and create_time  < ?                   " );

        putSql("selectDataRangeSql" ,"" + 
"select min(data_id) as min_id, max(data_id) as max_id from $(data) where data_id < (select max(data_id) from $(data))   " );

        putSql("updateStrandedBatches" ,"" + 
"update $(outgoing_batch) set status='OK' where node_id not                   " + 
"  in (select node_id from $(node) where sync_enabled=1) and status != 'OK'   " );

        putSql("deleteStrandedData" ,"" + 
"delete from $(data) where                                       " + 
"  data_id between :MIN and :MAX and                                        " + 
"  data_id < (select max(ref_data_id) from $(data)_ref) and      " + 
"  create_time < :CUTOFF_TIME and                                           " + 
"  data_id not in (select e.data_id from $(data_event) e where   " + 
"  e.data_id between :MIN and :MAX)                                         " );

        putSql("deleteDataSql" ,"" + 
"delete from $(data) where                                   " + 
"  data_id between :MIN and :MAX and                                    " + 
"  create_time < :CUTOFF_TIME and                                       " + 
"  data_id in (select e.data_id from $(data_event) e where   " + 
"  e.data_id between :MIN and :MAX)                                     " + 
"  and                                                                  " + 
"  data_id not in                                                       " + 
"  (select e.data_id from $(data_event) e where              " + 
"  e.data_id between :MIN and :MAX and                                  " + 
"  (e.data_id is null or                                                " + 
"  e.batch_id in                                                        " + 
"  (select batch_id from $(outgoing_batch) where             " + 
"  status not in ('OK','IG'))))                                         " );

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