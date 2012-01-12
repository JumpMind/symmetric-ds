package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;

public class NodeServiceSqlMap extends AbstractSqlMap {

    public NodeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("findSymmetricVersionSql" ,"" + 
"select symmetric_version from $(prefixName)_node where node_id in (select node_id from $(prefixName)_node_identity)   " );

        putSql("insertNodeIdentitySql" ,"" + 
"insert into $(prefixName)_node_identity values(?)   " );

        putSql("doesNodeGroupExistSql" ,"" + 
"select count(*) from $(prefixName)_node_group where node_group_id=?   " );

        putSql("insertNodeGroupSql" ,"" + 
"insert into $(prefixName)_node_group (description, node_group_id) values(?, ?)   " );

        putSql("nodeChannelControlIgnoreSql" ,"" + 
"update $(prefixName)_node_channel_ctl set ignore_enabled=? where node_id=? and   " + 
"  channel_id=?                                                              " );

        putSql("insertNodeChannelControlSql" ,"" + 
"insert into $(prefixName)_node_channel_ctl                                   " + 
"  (node_id,channel_id,ignore_enabled,suspend_enabled) values(?,?,?,?)   " );

        putSql("insertNodeSql" ,"" + 
"insert into $(prefixName)_node (node_id, node_group_id, external_id, created_at_node_id, timezone_offset, heartbeat_time) values (?, ?, ?, ?, ?, current_timestamp)   " );

        putSql("updateNodeSql" ,"" + 
"update $(prefixName)_node set node_group_id=?, external_id=?, database_type=?,                                                                       " + 
"  database_version=?, schema_version=?, symmetric_version=?, sync_url=?, heartbeat_time=?,                                                      " + 
"  sync_enabled=?, timezone_offset=?, batch_to_send_count=?, batch_in_error_count=?, created_at_node_id=?, deployment_type=? where node_id = ?   " );

        putSql("findNodeSql" ,"" + 
"where node_id = ?   " );

        putSql("findNodeByExternalIdSql" ,"" + 
"where node_group_id = ? and external_id = ? order by node_id   " );

        putSql("findEnabledNodesFromNodeGroupSql" ,"" + 
"where node_group_id = ? and sync_enabled=1 order by node_id   " );

        putSql("findNodesWithOpenRegistrationSql" ,"" + 
"where node_id in (select node_id from $(prefixName)_node_security where registration_enabled=1)   " );

        putSql("findNodesCreatedByMeSql" ,"" + 
"where created_at_node_id=? and created_at_node_id != node_id   " );

        putSql("findNodeSecuritySql" ,"" + 
"select node_id, node_password, registration_enabled, registration_time,                           " + 
"  initial_load_enabled, initial_load_time, created_at_node_id from $(prefixName)_node_security where   " + 
"  node_id = ?                                                                                     " );

        putSql("selectExternalIdsSql" ,"" + 
"select distinct(external_id) from $(prefixName)_node where sync_enabled=1 order by external_id asc   " );

        putSql("findAllNodeSecuritySql" ,"" + 
"select node_id, node_password, registration_enabled, registration_time,                     " + 
"  initial_load_enabled, initial_load_time, created_at_node_id from $(prefixName)_node_security   " );

        putSql("deleteNodeSecuritySql" ,"" + 
"delete from $(prefixName)_node_security where node_id = ?   " );

        putSql("deleteNodeSql" ,"" + 
"delete from $(prefixName)_node where node_id = ?   " );

        putSql("findNodeIdentitySql" ,"" + 
"inner join $(prefixName)_node_identity i on c.node_id =   " + 
"  i.node_id                                          " );

        putSql("deleteNodeIdentitySql" ,"" + 
"delete from $(prefixName)_node_identity   " );

        putSql("isNodeRegisteredSql" ,"" + 
"select count(*) from $(prefixName)_node_security s inner join                             " + 
"  $(prefixName)_node n on n.node_id=s.node_id where n.node_group_id=? and                 " + 
"  n.external_id=? and s.registration_time is not null and s.registration_enabled=0   " );

        putSql("findNodesWhoTargetMeSql" ,"" + 
"inner join $(prefixName)_node_group_link d on                                          " + 
"  c.node_group_id = d.source_node_group_id where d.target_node_group_id = ? and   " + 
"  d.data_event_action = ?                                                         " );

        putSql("findNodesWhoITargetSql" ,"" + 
"inner join $(prefixName)_node_group_link d on                                          " + 
"  c.node_group_id = d.target_node_group_id where d.source_node_group_id = ? and   " + 
"  d.data_event_action = ?                                                         " );

        putSql("selectNodeHostPrefixSql" ,"" + 
"select node_id, host_name, ip_address, os_user, os_name, os_arch, os_version, available_processors,        " + 
"  free_memory_bytes, total_memory_bytes, max_memory_bytes, java_version, java_vendor, symmetric_version,   " + 
"  timezone_offset, heartbeat_time, last_restart_time, create_time from $(prefixName)_node_host                  " );

        putSql("selectNodeHostByNodeIdSql" ,"" + 
"where node_id=?   " );

        putSql("selectNodePrefixSql" ,"" + 
"select c.node_id, c.node_group_id, c.external_id, c.sync_enabled, c.sync_url,                                                                                                                                    " + 
"  c.schema_version, c.database_type, c.database_version, c.symmetric_version, c.created_at_node_id, c.heartbeat_time, c.timezone_offset, c.batch_to_send_count, c.batch_in_error_count, c.deployment_type from   " + 
"  $(prefixName)_node c                                                                                                                                                                                                " );

        putSql("updateNodeSecuritySql" ,"" + 
"update $(prefixName)_node_security set node_password = ?, registration_enabled = ?,                                       " + 
"  registration_time = ?, initial_load_enabled = ?, initial_load_time = ?, created_at_node_id = ? where node_id = ?   " );

        putSql("insertNodeSecuritySql" ,"" + 
"insert into $(prefixName)_node_security (node_id, node_password, created_at_node_id) values (?, ?, ?)   " );

        putSql("getDataLoadStatusSql" ,"" + 
"select initial_load_enabled, initial_load_time from $(prefixName)_node_security ns,   " + 
"  $(prefixName)_node_identity ni where ns.node_id=ni.node_id                          " );

        putSql("insertNodeHostSql" ,"" + 
"insert into $(prefixName)_node_host                                                                                                                                                                                                                                            " + 
"  (ip_address, os_user, os_name, os_arch, os_version, available_processors, free_memory_bytes, total_memory_bytes, max_memory_bytes, java_version, java_vendor, symmetric_version, timezone_offset, heartbeat_time, last_restart_time, create_time, node_id, host_name)   " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp,?,?)                                                                                                                                                                                                            " );

        putSql("updateNodeHostSql" ,"" + 
"update $(prefixName)_node_host set                                                                                                          " + 
"  ip_address=?, os_user=?, os_name=?, os_arch=?, os_version=?, available_processors=?, free_memory_bytes=?,                            " + 
"  total_memory_bytes=?, max_memory_bytes=?, java_version=?, java_vendor=?, symmetric_version=?, timezone_offset=?, heartbeat_time=?,   " + 
"  last_restart_time=? where node_id=? and host_name=?                                                                                  " );

        putSql("findOfflineNodesSql" ,"" + 
"where sync_enabled = 1 and node_id != ? and created_at_node_id = ?   " );

    }

}