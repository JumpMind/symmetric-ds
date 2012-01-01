package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class PurgeServiceSqlMap extends AbstractSqlMap {

    public PurgeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectOutgoingBatchRangeSql" ,"" + 
"select min(batch_id), max(batch_id) from $(prefixName)_outgoing_batch where                                           " + 
"  create_time < ? and status in ('OK','IG') and batch_id < (select max(batch_id) from $(prefixName)_outgoing_batch)   " );

        putSql("deleteOutgoingBatchSql" ,"" + 
"delete from $(prefixName)_outgoing_batch where status in ('OK','IG') and batch_id between :MIN              " + 
"  and :MAX and batch_id not in (select batch_id from $(prefixName)_data_event where batch_id between :MIN   " + 
"  and :MAX)                                                                                            " );

        putSql("deleteDataEventSql" ,"" + 
"delete from $(prefixName)_data_event where batch_id not in (select batch_id from                     " + 
"  $(prefixName)_outgoing_batch where batch_id between :MIN and :MAX and status not in ('OK','IG'))   " + 
"  and batch_id between :MIN and :MAX                                                            " );

        putSql("deleteUnroutedDataEventSql" ,"" + 
"delete from $(prefixName)_data_event where           " + 
"  batch_id=-1 and create_time  < :CUTOFF_TIME   " );

        putSql("selectDataRangeSql" ,"" + 
"select min(data_id), max(data_id) from $(prefixName)_data where data_id < (select max(data_id) from $(prefixName)_data)   " );

        putSql("updateStrandedBatches" ,"" + 
"update $(prefixName)_outgoing_batch set status='OK' where node_id not                   " + 
"  in (select node_id from $(prefixName)_node where sync_enabled=1) and status != 'OK'   " );

        putSql("deleteStrandedData" ,"" + 
"delete from $(prefixName)_data where                                       " + 
"  data_id between :MIN and :MAX and                                   " + 
"  data_id < (select max(ref_data_id) from $(prefixName)_data_ref) and      " + 
"  create_time < :CUTOFF_TIME and                                      " + 
"  data_id not in (select e.data_id from $(prefixName)_data_event e where   " + 
"  e.data_id between :MIN and :MAX)                                    " );

        putSql("deleteDataSql" ,"" + 
"delete from $(prefixName)_data where                                   " + 
"  data_id between :MIN and :MAX and                               " + 
"  create_time < :CUTOFF_TIME and                                  " + 
"  data_id in (select e.data_id from $(prefixName)_data_event e where   " + 
"  e.data_id between :MIN and :MAX)                                " + 
"  and                                                             " + 
"  data_id not in                                                  " + 
"  (select e.data_id from $(prefixName)_data_event e where              " + 
"  e.data_id between :MIN and :MAX and                             " + 
"  (e.data_id is null or                                           " + 
"  e.batch_id in                                                   " + 
"  (select batch_id from $(prefixName)_outgoing_batch where             " + 
"  status not in ('OK','IG'))))                                    " );

        putSql("selectIncomingBatchRangeSql" ,"" + 
"select node_id, min(batch_id), max(batch_id) from $(prefixName)_incoming_batch where   " + 
"  create_time < ? and status = 'OK' group by node_id                              " );

        putSql("deleteIncomingBatchSql" ,"" + 
"delete from $(prefixName)_incoming_batch where batch_id between ? and ? and node_id =   " + 
"  ? and status = 'OK'                                                              " );

        putSql("deleteFromDataGapsSql" ,"" + 
"delete from $(prefixName)_data_gap where last_update_time < ? and status != 'GP'   " );

        putSql("deleteIncomingBatchByNodeSql" ,"" + 
"delete from $(prefixName)_incoming_batch where node_id = ?   " );

    }

}