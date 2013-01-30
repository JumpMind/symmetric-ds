package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class ConfigurationServiceSqlMap extends AbstractSqlMap {

    public ConfigurationServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        
        putSql("selectTableReloadRequest", "select reload_select, reload_delete_stmt, reload_enabled, reload_time, create_time, last_update_by, last_update_time from $(table_reload_request) where source_node_id=? and target_node_id=? and trigger_id=? and router_id=?");
        
        putSql("insertTableReloadRequest", "insert into $(table_reload_request) (reload_select, reload_delete_stmt, reload_enabled, reload_time, create_time, last_update_by, last_update_time, source_node_id, target_node_id, trigger_id, router_id) values (?,?,?,?,?,?,?,?,?,?,?)");

        putSql("updateTableReloadRequest", "update $(table_reload_request) set reload_select=?, reload_delete_stmt=?, reload_enabled=?, reload_time=?, create_time=?, last_update_by=?, last_update_time=? where source_node_id=? and target_node_id=? and trigger_id=? and router_id=?");
        
        putSql("selectDataEventActionsByIdSql",
                " select data_event_action from $(node_group_link) where         "
              + "   source_node_group_id = ? and target_node_group_id = ?        ");

        putSql("groupsLinksSql", ""
                + "select source_node_group_id, target_node_group_id, data_event_action from   "
                + "  $(node_group_link)                                                        ");

        putSql("updateNodeGroupSql", 
                  " update $(node_group) set description=? where                          "
                + "  node_group_id=?                                                      ");

        putSql("insertNodeGroupSql", 
                  "insert into $(node_group)                                      "
                + "  (description, node_group_id) values(?,?)                     ");

        putSql("updateNodeGroupLinkSql", ""
                + "update $(node_group_link) set data_event_action=? where   "
                + "  source_node_group_id=? and target_node_group_id=?             ");

        putSql("insertNodeGroupLinkSql",
                ""
                        + "insert into $(node_group_link)                                              "
                        + "  (data_event_action, source_node_group_id, target_node_group_id) values(?,?,?)   ");

        putSql("selectNodeGroupsSql", ""
                + "select node_group_id, description from $(node_group)   ");

        putSql("groupsLinksForSql",
                "select source_node_group_id, target_node_group_id, data_event_action from   "
                        + "  $(node_group_link) where source_node_group_id = ?                   ");

        putSql("isChannelInUseSql", "select count(*) from $(trigger) where channel_id = ?   ");
        
        putSql("selectChannelsSql",
          "select c.channel_id, c.processing_order, c.max_batch_size, c.enabled,    " +
          "  c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route,     " +
          "  c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob,   " +
          "  c.batch_algorithm, c.extract_period_millis, c.data_loader_type         " +
          " from $(channel) c order by c.processing_order asc, c.channel_id         ");

        putSql("selectNodeChannelsSql",
          "select c.channel_id, nc.node_id, nc.ignore_enabled, nc.suspend_enabled, c.processing_order,       "
        + "  c.max_batch_size, c.enabled, c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route, " 
        + "  c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob, c.batch_algorithm,         " 
        + "  nc.last_extract_time, c.extract_period_millis, c.data_loader_type                               "
        + "  from $(channel) c left outer join                                                               "
        + "  $(node_channel_ctl) nc on c.channel_id = nc.channel_id and nc.node_id = ?                       "
        + "  order by c.processing_order asc, c.channel_id                                                   ");

        putSql("selectNodeChannelControlLastExtractTimeSql", ""
         + "select channel_id, last_extract_time                 "
         + "  from $(node_channel_ctl) where node_id = ?   "
         + "  order by channel_id                                ");

        putSql("insertChannelSql",
           "insert into $(channel) (channel_id, processing_order, max_batch_size,                 "
         + "  max_batch_to_send, max_data_to_route, use_old_data_to_route, use_row_data_to_route, " 
         + "  use_pk_data_to_route, contains_big_lob, enabled, batch_algorithm, description,      " 
         + "  extract_period_millis, data_loader_type)                                            "
         + "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?)                                ");

        putSql("updateChannelSql",
           "update $(channel) set processing_order=?, max_batch_size=?,                                                              "
         + "  max_batch_to_send=?, max_data_to_route=?, use_old_data_to_route=?, use_row_data_to_route=?,                            "
         + "  use_pk_data_to_route=?, contains_big_lob=?, enabled=?, batch_algorithm=?, extract_period_millis=?, " 
         + "  data_loader_type=?" 
         + " where channel_id=?   ");

        putSql("deleteNodeGroupLinkSql",
           "delete from $(node_group_link) where source_node_group_id=? and target_node_group_id=?   ");

        putSql("deleteNodeGroupSql", "delete from $(node_group) where node_group_id=?   ");

        putSql("deleteChannelSql", "delete from $(channel) where channel_id=?   ");

        putSql("deleteNodeChannelSql", "delete from $(node_channel_ctl) where channel_id=?   ");

        putSql("selectNodeGroupChannelWindowSql",
                "select node_group_id, channel_id, start_time, end_time, enabled                    "
              + "  from $(node_group_channel_window) where node_group_id=? and channel_id=?   ");

        putSql("insertNodeChannelControlSql", ""
                + "insert into $(node_channel_ctl) (node_id, channel_id,                         "
                + "  suspend_enabled, ignore_enabled,last_extract_time) values (?, ?, ?, ?, ?)   ");

        putSql("updateNodeChannelControlSql",
               "update $(node_channel_ctl) set                                                              "
             + "  suspend_enabled=?, ignore_enabled=?, last_extract_time=? where node_id=? and channel_id=? ");

        putSql("getRegistrationRedirectSql",
            "select registrant_external_id, registration_node_id from $(registration_redirect)");

    }

}