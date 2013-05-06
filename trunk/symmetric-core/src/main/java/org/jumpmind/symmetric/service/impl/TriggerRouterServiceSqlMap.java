package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class TriggerRouterServiceSqlMap extends AbstractSqlMap {

    public TriggerRouterServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off

        putSql("countTriggerRoutersByRouterIdSql",
           "select count(*) from $(trigger_router) where router_id=?   ");

        putSql("countTriggerRoutersByTriggerIdSql", ""
                + "select count(*) from $(trigger_router) where trigger_id=?   ");

        putSql("countTriggerByTriggerIdSql", ""
                + "select count(*) from $(trigger) where trigger_id=?   ");

        putSql("countTriggerByTableNameSql", ""
                + "select count(*) from $(trigger) where source_table_name=?   ");

        putSql("deleteRouterSql", "" + "delete from $(router) where router_id=?   ");

        putSql("inactivateTriggerHistorySql",
                ""
                        + "update $(trigger_hist) set inactive_time = current_timestamp, error_message=? where   "
                        + "  trigger_hist_id=?                                                                         ");

        putSql("selectTriggersSql", "" + "from $(trigger) t order by trigger_id asc   ");

        putSql("selectTriggerRoutersSql", ""
                + "from $(trigger_router) tr                                 "
                + "  inner join $(trigger) t on tr.trigger_id=t.trigger_id   "
                + "  inner join $(router) r on tr.router_id=r.router_id      ");

        putSql("selectTriggerRoutersColumnList",
                ""
                        + "  tr.create_time,tr.last_update_time,tr.last_update_by,tr.initial_load_order, tr.initial_load_select, tr.initial_load_delete_stmt, tr.ping_back_enabled, tr.enabled   ");

        putSql("selectRoutersColumnList",
                ""
                        + "  r.sync_on_insert as r_sync_on_insert,r.sync_on_update as r_sync_on_update,r.sync_on_delete as r_sync_on_delete,                            "
                        + "  r.target_catalog_name,r.source_node_group_id,r.target_schema_name,r.target_table_name,r.target_node_group_id,r.router_expression,        "
                        + "  r.router_type,r.router_id,r.create_time as r_create_time,r.last_update_time as r_last_update_time,r.last_update_by as r_last_update_by   ");

        putSql("selectTriggersColumnList",
                ""
                        + "  t.trigger_id,t.channel_id,t.source_table_name,t.source_schema_name,t.source_catalog_name,        "
                        + "  t.sync_on_insert,t.sync_on_update,t.sync_on_delete,t.sync_on_incoming_batch,t.use_stream_lobs,   "
                        + "  t.use_capture_lobs,t.use_capture_old_data,t.use_handle_key_updates,                              "
                        + "  t.excluded_column_names, t.sync_key_names,                                                       "
                        + "  t.name_for_delete_trigger,t.name_for_insert_trigger,t.name_for_update_trigger,                   "
                        + "  t.sync_on_insert_condition,t.sync_on_update_condition,t.sync_on_delete_condition,                "
                        + "  t.tx_id_expression,t.external_select,t.create_time as t_create_time,                             "
                        + "  t.last_update_time as t_last_update_time, t.last_update_by as t_last_update_by                   ");

        putSql("selectTriggerNameInUseSql",
                ""
                        + "select count(*) from $(trigger_hist) where (name_for_update_trigger=? or name_for_insert_trigger=? or name_for_delete_trigger=?) and trigger_id != ? and inactive_time is null   ");

        putSql("selectGroupTriggersSql", ""
                + "where r.source_node_group_id = ? or r.target_node_group_id = ? order by t.channel_id   ");

        putSql("activeTriggersForSourceNodeGroupSql", "" + "where r.source_node_group_id = ?   ");

        putSql("activeTriggersForReloadSql", ""
                + "where r.source_node_group_id = ? and                          "
                + "  r.target_node_group_id = ? and t.channel_id != ? and tr.enabled=1 order by   "
                + "  tr.initial_load_order                                       ");

        putSql("activeTriggerHistSql", "" + "where inactive_time is null   ");

        putSql("allTriggerHistSql",
                ""
                        + "select trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,trigger_row_hash,error_message   "
                        + "  from $(trigger_hist)                                                                                                                                                                                                                                                      ");

        putSql("triggerHistBySourceTableWhereSql", ""
                + "where source_table_name=? and inactive_time is null   ");

        putSql("latestTriggerHistSql",
                ""
                        + "select                                                                                                                                                                                                                                                                       "
                        + "  trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,trigger_row_hash,error_message   "
                        + "  from $(trigger_hist) where trigger_hist_id = (select max(trigger_hist_id)                                                                                                                                                                                            "
                        + "  from $(trigger_hist) where trigger_id=?)                                                                                                                                                                                                                             ");

        putSql("latestTriggerHistSqlForIdAndName",
                ""
                        + "select                                                                                                                                                                                                                                                                       "
                        + "  trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,trigger_row_hash,error_message   "
                        + "  from $(trigger_hist) where trigger_hist_id = (select max(trigger_hist_id)                                                                                                                                                                                            "
                        + "  from $(trigger_hist) where trigger_id=? and source_table_name=?)                                                                                                                                                                                                                             ");

        putSql("triggerHistSql",
                ""
                        + "select                                                                                                                                                                                                                                                                       "
                        + "  trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,trigger_row_hash,error_message   "
                        + "  from $(trigger_hist) where trigger_hist_id = ?                                                                                                                                                                                                                       ");

        putSql("insertTriggerHistorySql",
                ""
                        + "insert into $(trigger_hist)                                                                                                                                                                                                                              "
                        + "  (trigger_id,source_table_name,table_hash,create_time,column_names,pk_column_names,last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,trigger_row_hash,error_message)   "
                        + "  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)                                                                                                                                                                                                                          ");

        putSql("deleteTriggerSql", "" + "delete from $(trigger) where trigger_id=?   ");

        putSql("deleteTriggerHistorySql", ""
                + "delete from $(trigger_hist) where trigger_hist_id=?   ");

        putSql("insertTriggerSql",
                ""
                        + "insert into $(trigger)                                                                                                         "
                        + "  (source_catalog_name,source_schema_name,source_table_name,channel_id,sync_on_update,sync_on_insert,sync_on_delete,                 "
                        + "  sync_on_incoming_batch,use_stream_lobs,use_capture_lobs,use_capture_old_data,use_handle_key_updates,name_for_update_trigger,name_for_insert_trigger,name_for_delete_trigger,   "
                        + "  sync_on_update_condition,sync_on_insert_condition,sync_on_delete_condition,tx_id_expression,excluded_column_names, sync_key_names,            "
                        + "  create_time,last_update_by,last_update_time,external_select,trigger_id)                                                            "
                        + "  values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)                                                                             ");

        putSql("updateTriggerSql",
                ""
                        + "update $(trigger)                                                                                                                                "
                        + "  set source_catalog_name=?,source_schema_name=?,source_table_name=?,                                                                            "
                        + "  channel_id=?,sync_on_update=?,sync_on_insert=?,sync_on_delete=?,                                                                               "
                        + "  sync_on_incoming_batch=?,use_stream_lobs=?,use_capture_lobs=?,use_capture_old_data=?,use_handle_key_updates=?,name_for_update_trigger=?,name_for_insert_trigger=?,        "
                        + "  name_for_delete_trigger=?,sync_on_update_condition=?,sync_on_insert_condition=?,                                              "
                        + "  sync_on_delete_condition=?,tx_id_expression=?,excluded_column_names=?,sync_key_names=?,last_update_by=?,last_update_time=?,external_select=?   "
                        + "  where trigger_id=?                                                                                                            ");

        putSql("insertRouterSql",
                ""
                        + "insert into $(router)                                                                                                           "
                        + "  (target_catalog_name,target_schema_name,target_table_name,source_node_group_id,target_node_group_id,                                "
                        + "  router_type,router_expression,sync_on_update,sync_on_insert,sync_on_delete,create_time,last_update_by,last_update_time,router_id)   "
                        + "  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)                                                                                                 ");

        putSql("updateRouterSql",
                ""
                        + "update $(router)                                                                                           "
                        + "  set target_catalog_name=?,target_schema_name=?,target_table_name=?,source_node_group_id=?,                     "
                        + "  target_node_group_id=?,router_type=?,router_expression=?,sync_on_update=?,sync_on_insert=?,sync_on_delete=?,   "
                        + "  last_update_by=?,last_update_time=?                                                                            "
                        + "  where router_id=?                                                                                              ");

        putSql("deleteTriggerRouterSql", ""
                + "delete from $(trigger_router) where trigger_id=? and router_id=?   ");

        putSql("insertTriggerRouterSql",
                ""
                        + "insert into $(trigger_router)                                                                                             "
                        + "  (initial_load_order,initial_load_select, initial_load_delete_stmt, ping_back_enabled,create_time,last_update_by,last_update_time,enabled,trigger_id,router_id)   "
                        + "  values(?,?,?,?,?,?,?,?,?,?)                                                                                                       ");

        putSql("updateTriggerRouterSql",
                ""
                        + "update $(trigger_router)                                                                             "
                        + "  set initial_load_order=?,initial_load_select=?,initial_load_delete_stmt=?, ping_back_enabled=?,last_update_by=?,last_update_time=?,enabled=?   "
                        + "  where trigger_id=? and router_id=?                                                                       ");

        putSql("selectTriggerTargetSql",
                ""
                        + "where t.source_table_name = ? and r.target_node_group_id = ? and t.channel_id = ? and r.source_node_group_id = ?   ");

        putSql("selectTriggerRouterSql", "" + "where t.trigger_id=? and r.router_id=?   ");

        putSql("selectRouterSql", "" + "from $(router) r where r.router_id=?   ");

        putSql("selectRoutersSql", "" + "from $(router) r order by r.router_id   ");

        putSql("selectRouterByNodeGroupLinkWhereSql",
                "from $(router) r where r.source_node_group_id=? and r.target_node_group_id=? order by r.router_id   ");

        putSql("selectTriggerByIdSql", "" + "where t.trigger_id = ?   ");

        putSql("selectMaxTriggerLastUpdateTime" ,"select max(last_update_time) from $(trigger) where last_update_time is not null" );
        putSql("selectMaxRouterLastUpdateTime" ,"select max(last_update_time) from $(router) where last_update_time is not null" );
        putSql("selectMaxTriggerRouterLastUpdateTime" ,"select max(last_update_time) from $(trigger_router) where last_update_time is not null" );


    }

}