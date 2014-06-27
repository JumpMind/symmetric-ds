package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class RouterServiceSqlMap extends AbstractSqlMap {

    public RouterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("selectDataUsingGapsSql",
                ""
                        + "select $(selectDataUsingGapsSqlHint) d.data_id, d.table_name, d.event_type, d.row_data, d.pk_data, d.old_data,                        "
                        + "  d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data   "
                        + "  from $(data) d where d.channel_id=? $(dataRange)                                                "
                        + "  order by d.data_id asc                                                                                ");

        putSql("selectDistinctDataIdFromDataEventSql",
                ""
                        + "select distinct(data_id) from $(data_event) where data_id > ? order by data_id asc   ");

        putSql("selectDistinctDataIdFromDataEventUsingGapsSql",
                ""
                        + "select distinct(data_id) from $(data_event) where data_id >=? and data_id <= ? order by data_id asc   ");

        putSql("selectUnroutedCountForChannelSql", ""
                + "select count(*) from $(data) where channel_id=? and data_id >=?   ");

        putSql("selectLastDataIdRoutedUsingDataGapSql", ""
                + "select max(start_id) from $(data_gap)   ");

    }

}