package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class ConfigurationServiceSqlMap extends AbstractSqlMap {

    public ConfigurationServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectDataEventActionsByIdSql" ,"" + 
"select data_event_action from $(prefixName)_node_group_link where   " + 
"  source_node_group_id = ? and target_node_group_id = ?        " );

        putSql("groupsLinksSql" ,"" + 
"select source_node_group_id, target_node_group_id, data_event_action from   " + 
"  $(prefixName)_node_group_link                                                  " );

        putSql("updateNodeGroupSql" ,"" + 
"update $(prefixName)_node_group set description=? where   " + 
"  node_group_id=?                                    " );

        putSql("insertNodeGroupSql" ,"" + 
"insert into $(prefixName)_node_group              " + 
"  (description, node_group_id) values(?,?)   " );

        putSql("updateNodeGroupLinkSql" ,"" + 
"update $(prefixName)_node_group_link set data_event_action=? where   " + 
"  source_node_group_id=? and target_node_group_id=?             " );

        putSql("insertNodeGroupLinkSql" ,"" + 
"insert into $(prefixName)_node_group_link                                              " + 
"  (data_event_action, source_node_group_id, target_node_group_id) values(?,?,?)   " );

        putSql("selectNodeGroupsSql" ,"" + 
"select node_group_id, description from $(prefixName)_node_group   " );

        putSql("groupsLinksForSql" ,"" + 
"select source_node_group_id, target_node_group_id, data_event_action from   " + 
"  $(prefixName)_node_group_link where source_node_group_id = ?                   " );

        putSql("isChannelInUseSql" ,"" + 
"select count(*) from $(prefixName)_trigger where channel_id = ?   " );

        putSql("selectChannelsSql" ,"" + 
"select c.channel_id, nc.node_id, nc.ignore_enabled, nc.suspend_enabled, c.processing_order,                                                                                                                                               " + 
"  c.max_batch_size, c.enabled, c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route, c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob, c.batch_algorithm, nc.last_extract_time, c.extract_period_millis   " + 
"  from $(prefixName)_channel c left outer join                                                                                                                                                                                                 " + 
"  $(prefixName)_node_channel_ctl nc on c.channel_id = nc.channel_id and nc.node_id = ?                                                                                                                                                         " + 
"  order by c.processing_order asc, c.channel_id                                                                                                                                                                                           " );

        putSql("selectNodeChannelControlLastExtractTimeSql" ,"" + 
"select channel_id, last_extract_time                 " + 
"  from $(prefixName)_node_channel_ctl where node_id = ?   " + 
"  order by channel_id                                " );

        putSql("insertChannelSql" ,"" + 
"insert into $(prefixName)_channel (channel_id, processing_order, max_batch_size,                                                                                                                   " + 
"  max_batch_to_send, max_data_to_route, use_old_data_to_route, use_row_data_to_route, use_pk_data_to_route, contains_big_lob, enabled, batch_algorithm, description, extract_period_millis)   " + 
"  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null,? )                                                                                                                                           " );

        putSql("updateChannelSql" ,"" + 
"update $(prefixName)_channel set processing_order=?, max_batch_size=?,                                                        " + 
"  max_batch_to_send=?, max_data_to_route=?, use_old_data_to_route=?, use_row_data_to_route=?,                            " + 
"  use_pk_data_to_route=?, contains_big_lob=?, enabled=?, batch_algorithm=?, extract_period_millis=? where channel_id=?   " );

        putSql("deleteNodeGroupLinkSql" ,"" + 
"delete from $(prefixName)_node_group_link where source_node_group_id=? and target_node_group_id=?   " );

        putSql("deleteNodeGroupSql" ,"" + 
"delete from $(prefixName)_node_group where node_group_id=?   " );

        putSql("deleteChannelSql" ,"" + 
"delete from $(prefixName)_channel where channel_id=?   " );

        putSql("deleteNodeChannelSql" ,"" + 
"delete from $(prefixName)_node_channel_ctl where channel_id=?   " );

        putSql("selectNodeGroupChannelWindowSql" ,"" + 
"select node_group_id, channel_id, start_time, end_time, enabled                    " + 
"  from $(prefixName)_node_group_channel_window where node_group_id=? and channel_id=?   " );

        putSql("insertNodeChannelControlSql" ,"" + 
"insert into $(prefixName)_node_channel_ctl (node_id, channel_id,                   " + 
"  suspend_enabled, ignore_enabled,last_extract_time) values (?, ?, ?, ?, ?)   " );

        putSql("updateNodeChannelControlSql" ,"" + 
"update $(prefixName)_node_channel_ctl set                                                          " + 
"  suspend_enabled=?, ignore_enabled=?, last_extract_time=? where node_id=? and channel_id=?   " );

        putSql("getRegistrationRedirectSql" ,"" + 
        "select registrant_external_id, registration_node_id from $(prefixName)_registration_redirect   " );

    }

}