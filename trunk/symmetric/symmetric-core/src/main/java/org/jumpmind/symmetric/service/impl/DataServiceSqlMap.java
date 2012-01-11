package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class DataServiceSqlMap extends AbstractSqlMap {

    public DataServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectEventDataToExtractSql" ,"" + 
"select d.data_id, d.table_name, d.event_type, d.row_data, d.pk_data, d.old_data,                                                                     " + 
"  d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data, e.router_id from $(prefixName)_data d inner join   " + 
"  $(prefixName)_data_event e on d.data_id = e.data_id inner join $(prefixName)_outgoing_batch o on o.batch_id=e.batch_id                                       " + 
"  where o.batch_id = ?                                                                                                                               " );

        putSql("selectEventDataIdsSql" ,"" + 
"select d.data_id from $(prefixName)_data d inner join                                                                 " + 
"  $(prefixName)_data_event e on d.data_id = e.data_id inner join $(prefixName)_outgoing_batch o on o.batch_id=e.batch_id   " + 
"  where o.batch_id = ?                                                                                           " );

        putSql("selectMaxDataEventDataIdSql" ,"" + 
"select max(data_id) from $(prefixName)_data_event   " );

        putSql("checkForAndUpdateMissingChannelIdSql" ,"" + 
"update $(prefixName)_data set channel_id=?                           " + 
"  where                                                         " + 
"  data_id >= ? and data_id <= ? and                             " + 
"  channel_id not in (select channel_id from $(prefixName)_channel)   " );

        putSql("countDataInRangeSql" ,"" + 
"select count(*) from $(prefixName)_data where data_id > ? and data_id < ?   " );

        putSql("insertIntoDataSql" ,"" + 
"insert into $(prefixName)_data (data_id, table_name, event_type, row_data, pk_data,                               " + 
"  old_data, trigger_hist_id, channel_id, create_time) values(null, ?, ?, ?, ?, ?, ?, ?, current_timestamp)   " );

        putSql("insertIntoDataEventSql" ,"" + 
"insert into $(prefixName)_data_event (data_id, batch_id, router_id, create_time) values(?, ?, ?, current_timestamp)   " );

        putSql("findDataEventCreateTimeSql" ,"" + 
"select max(create_time) from $(prefixName)_data_event where data_id=?   " );

        putSql("findDataCreateTimeSql" ,"" + 
"select create_time from $(prefixName)_data where data_id=?   " );

        putSql("findDataGapsByStatusSql" ,"" + 
"select start_id, end_id, create_time from $(prefixName)_data_gap where status=? order by start_id asc   " );

        putSql("insertDataGapSql" ,"" + 
"insert into $(prefixName)_data_gap (status, last_update_hostname, start_id, end_id, last_update_time, create_time) values(?, ?, ?, ?, current_timestamp, current_timestamp)   " );

        putSql("updateDataGapSql" ,"" + 
"update $(prefixName)_data_gap set status=?, last_update_hostname=?, last_update_time=current_timestamp where start_id=? and end_id=?   " );

        putSql("selectMaxDataIdSql" ,"" + 
"select max(data_id) from $(prefixName)_data   " );

    }

}