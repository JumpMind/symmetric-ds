/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class NodeServiceSqlMap extends AbstractSqlMap {

    public NodeServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("findSymmetricVersionSql",
                "select symmetric_version from $(node)                      "
                        + "  where node_id in (select node_id from $(node_identity))  ");

        putSql("insertNodeIdentitySql", "" + "insert into $(node_identity) values(?)   ");

        putSql("doesNodeGroupExistSql", ""
                + "select count(*) from $(node_group) where node_group_id=?   ");

        putSql("insertNodeGroupSql", ""
                + "insert into $(node_group) (description, node_group_id) values(?, ?)   ");

        putSql("nodeChannelControlIgnoreSql", ""
                + "update $(node_channel_ctl) set ignore_enabled=? where node_id=? and   "
                + "  channel_id=?                                                              ");

        putSql("insertNodeChannelControlSql", ""
                + "insert into $(node_channel_ctl)                                   "
                + "  (node_id,channel_id,ignore_enabled,suspend_enabled) values(?,?,?,?)   ");

        putSql("insertNodeSql",
                "insert into $(node) (node_group_id, external_id, database_type, database_version, schema_version, symmetric_version, sync_url," +
                "heartbeat_time, sync_enabled, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id, " +
                "deployment_type, node_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        putSql("updateNodeSql",
                "update $(node) set node_group_id=?, external_id=?, database_type=?,                                                                       "
                        + "  database_version=?, schema_version=?, symmetric_version=?, sync_url=?, heartbeat_time=?,                                                      "
                        + "  sync_enabled=?, timezone_offset=?, batch_to_send_count=?, batch_in_error_count=?, created_at_node_id=?, deployment_type=? where node_id = ?   ");

        putSql("findNodeSql", "where node_id = ?   ");

        putSql("findNodeByExternalIdSql", ""
                + "where node_group_id = ? and external_id = ? order by node_id   ");

        putSql("findEnabledNodesFromNodeGroupSql", ""
                + "where node_group_id = ? and sync_enabled=1 order by node_id   ");

        putSql("findNodesWithOpenRegistrationSql",
                "where node_id in (select node_id from $(node_security) where registration_enabled=1)   ");

        putSql("findNodesCreatedByMeSql", ""
                + "where created_at_node_id=? and created_at_node_id != node_id   ");

        putSql("findNodeSecuritySql",
                "select node_id, node_password, registration_enabled, registration_time,                          "
                        + "  initial_load_enabled, initial_load_time, created_at_node_id,                         "
                        + " rev_initial_load_enabled, rev_initial_load_time, initial_load_id, " +
                          " initial_load_create_by, rev_initial_load_id, rev_initial_load_create_by " +
                          " from $(node_security) where   "
                        + "  node_id = ?");

        putSql("selectExternalIdsSql",
                "select distinct(external_id) from $(node) where sync_enabled=1 order by external_id asc   ");
        
        putSql("findNodeSecurityWithLoadEnabledSql",
                "select node_id, node_password, registration_enabled, registration_time,                   "
                        + " initial_load_enabled, initial_load_time, created_at_node_id,                   "
                        + " rev_initial_load_enabled, rev_initial_load_time, initial_load_id, " +
                          " initial_load_create_by, rev_initial_load_id, rev_initial_load_create_by " +
                          " from $(node_security)          "
                        + " where initial_load_enabled=1 or rev_initial_load_enabled=1                     ");
        
        putSql("findAllNodeSecuritySql",
                "select node_id, node_password, registration_enabled, registration_time,                   "
                        + " initial_load_enabled, initial_load_time, created_at_node_id,                   "
                        + " rev_initial_load_enabled, rev_initial_load_time, initial_load_id, " +
                          " initial_load_create_by, rev_initial_load_id, rev_initial_load_create_by " +
                          " from $(node_security)   ");

        putSql("deleteNodeSecuritySql", "delete from $(node_security) where node_id = ?");

        putSql("deleteNodeSql", "delete from $(node) where node_id = ?");
        
        putSql("deleteNodeHostSql", "delete from $(node_host) where node_id = ?");

        putSql("findNodeIdentitySql", "inner join $(node_identity) i on c.node_id =   "
                + "  i.node_id                                          ");

        putSql("deleteNodeIdentitySql", "delete from $(node_identity)   ");

        putSql("isNodeRegisteredSql",
                "select count(*) from $(node_security) s inner join                             "
                        + "  $(node) n on n.node_id=s.node_id where n.node_group_id=? and                 "
                        + "  n.external_id=? and s.registration_time is not null and s.registration_enabled=0   ");

        putSql("findNodesWhoTargetMeSql",
                ""
                        + "inner join $(node_group_link) d on                                          "
                        + "  c.node_group_id = d.source_node_group_id where d.target_node_group_id = ? and   "
                        + "  d.data_event_action = ? and c.node_id not in (select node_id from $(node_identity))          ");

        putSql("findNodesWhoITargetSql",
                ""
                        + "inner join $(node_group_link) d on                                          "
                        + "  c.node_group_id = d.target_node_group_id where d.source_node_group_id = ? and   "
                        + "  d.data_event_action = ? and c.node_id not in (select node_id from $(node_identity))   ");

        putSql("selectNodeHostPrefixSql",
                ""
                        + "select node_id, host_name, ip_address, os_user, os_name, os_arch, os_version, available_processors,        "
                        + "  free_memory_bytes, total_memory_bytes, max_memory_bytes, java_version, java_vendor, symmetric_version,   "
                        + "  timezone_offset, heartbeat_time, last_restart_time, create_time from $(node_host) h");

        putSql("selectNodeHostByNodeIdSql", "where node_id=?");

        putSql("selectNodePrefixSql",
                          "select c.node_id, c.node_group_id, c.external_id, c.sync_enabled, c.sync_url,                                                                                                                                    "
                        + "  c.schema_version, c.database_type, c.database_version, c.symmetric_version, c.created_at_node_id, c.heartbeat_time, c.timezone_offset, c.batch_to_send_count, c.batch_in_error_count, c.deployment_type from   "
                        + "  $(node) c                                                                                                                                                                                                ");

        putSql("updateNodeSecuritySql",
                ""
                        + "update $(node_security) set node_password = ?, registration_enabled = ?,                                       "
                        + "  registration_time = ?, initial_load_enabled = ?, initial_load_time = ?, created_at_node_id = ?,"
                        + "  rev_initial_load_enabled=?, rev_initial_load_time=?, initial_load_id=?, " +
                          " initial_load_create_by=?, rev_initial_load_id=?, rev_initial_load_create_by=? " +
                          " where node_id = ?   ");

        putSql("insertNodeSecuritySql",
                ""
                        + "insert into $(node_security) (node_id, node_password, created_at_node_id) values (?, ?, ?)   ");

        putSql("getDataLoadStatusSql", ""
                + "select initial_load_enabled, initial_load_time from $(node_security) ns,   "
                + "  $(node_identity) ni where ns.node_id=ni.node_id                          ");

        putSql("insertNodeHostSql",
                ""
                        + "insert into $(node_host)                                                                                                                                                                                                                                            "
                        + "  (ip_address, os_user, os_name, os_arch, os_version, available_processors, free_memory_bytes, total_memory_bytes, max_memory_bytes, java_version, java_vendor, symmetric_version, timezone_offset, heartbeat_time, last_restart_time, create_time, node_id, host_name)   "
                        + "  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp,?,?)                                                                                                                                                                                                            ");

        putSql("updateNodeHostSql",
                ""
                        + "update $(node_host) set                                                                                                          "
                        + "  ip_address=?, os_user=?, os_name=?, os_arch=?, os_version=?, available_processors=?, free_memory_bytes=?,                            "
                        + "  total_memory_bytes=?, max_memory_bytes=?, java_version=?, java_vendor=?, symmetric_version=?, timezone_offset=?, heartbeat_time=?,   "
                        + "  last_restart_time=? where node_id=? and host_name=?                                                                                  ");

        putSql("findOfflineNodesSql", 
                "select h.node_id, max(h.heartbeat_time) as heartbeat_time, h.timezone_offset from $(node_host) h inner join $(node) n on h.node_id=n.node_id"
              + " where n.sync_enabled = 1 and n.node_id != ? and n.created_at_node_id = ? group by h.node_id, h.timezone_offset");

    }

}