package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;

public class RouterServiceSqlMap extends AbstractSqlMap {

    public RouterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("selectDataToBatchSql",
                ""
                        + "select d.data_id, d.table_name, d.event_type, d.row_data, d.pk_data, d.old_data,                                                                        "
                        + "  d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data, e.data_id from $(prefixName)_data d left outer join   "
                        + "  $(prefixName)_data_event e on d.data_id=e.data_id where d.channel_id=? and d.data_id > ? order by d.data_id asc                                            ");

        putSql("selectDataUsingGapsSql",
                ""
                        + "select d.data_id, d.table_name, d.event_type, d.row_data, d.pk_data, d.old_data,                        "
                        + "  d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data   "
                        + "  from $(prefixName)_data d where d.channel_id=? $(dataRange)                                                "
                        + "  order by d.data_id asc                                                                                ");

        putSql("selectDistinctDataIdFromDataEventSql",
                ""
                        + "select distinct(data_id) from $(prefixName)_data_event where data_id > ? order by data_id asc   ");

        putSql("selectDistinctDataIdFromDataEventUsingGapsSql",
                ""
                        + "select distinct(data_id) from $(prefixName)_data_event where data_id >=? and data_id <= ? order by data_id asc   ");

        putSql("selectUnroutedCountForChannelSql", ""
                + "select count(*) from $(prefixName)_data where channel_id=? and data_id >=?   ");

        putSql("selectLastDataIdRoutedUsingDataRefSql", ""
                + "select max(ref_data_id) from $(prefixName)_data_ref   ");

        putSql("selectLastDataIdRoutedUsingDataGapSql", ""
                + "select max(end_id) from $(prefixName)_data_gap   ");

    }

}