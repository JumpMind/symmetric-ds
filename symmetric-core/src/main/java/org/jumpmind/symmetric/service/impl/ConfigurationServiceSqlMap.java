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

public class ConfigurationServiceSqlMap extends AbstractSqlMap {

    public ConfigurationServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        
        putSql("updateNodeChannelLastExtractTime", "update $(node_channel_ctl) set last_extract_time=? where channel_id=? and node_id=?");

        putSql("selectDataEventActionsByIdSql",
                " select data_event_action from $(node_group_link) where         "
              + "   source_node_group_id = ? and target_node_group_id = ?        ");

        putSql("groupsLinksSql", ""
                + "select source_node_group_id, target_node_group_id, data_event_action, sync_config_enabled, last_update_time, last_update_by, create_time from   "
                + "  $(node_group_link) order by source_node_group_id  ");

        putSql("updateNodeGroupSql",
                  " update $(node_group) set description=?, last_update_time=?, last_update_by=? where "
                + "  node_group_id=?                                                      ");

        putSql("insertNodeGroupSql",
                  "insert into $(node_group)                                      "
                + "  (description, node_group_id, last_update_time, last_update_by, create_time) values(?,?,?,?,?)                     ");

        putSql("updateNodeGroupLinkSql", ""
                + "update $(node_group_link) set data_event_action=?, sync_config_enabled=?, last_update_time=?, last_update_by=? where   "
                + "  source_node_group_id=? and target_node_group_id=?             ");

        putSql("insertNodeGroupLinkSql",
                         "insert into $(node_group_link)                                              "
                        + "  (data_event_action, source_node_group_id, target_node_group_id, sync_config_enabled, last_update_time, last_update_by, create_time) values(?,?,?,?,?,?,?)");

        putSql("selectNodeGroupsSql", ""
                + "select node_group_id, description, last_update_time, last_update_by, create_time from $(node_group) order by node_group_id   ");

        putSql("groupsLinksForSql",
                "select source_node_group_id, target_node_group_id, data_event_action, sync_config_enabled, last_update_time, last_update_by, create_time from   "
                        + "  $(node_group_link) where source_node_group_id = ?                   ");

        putSql("countGroupLinksForSql","select count(*) from $(node_group_link) where source_node_group_id = ? and target_node_group_id = ?");
        
        putSql("isChannelInUseSql", "select count(*) from $(trigger) where channel_id = ?   ");

        putSql("selectChannelsSql",
          "select c.channel_id, c.processing_order, c.max_batch_size, c.enabled,                   " +
          "  c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route,                    " +
          "  c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob,                  " +
          "  c.batch_algorithm, c.extract_period_millis, c.data_loader_type,                       " +
          "  c.last_update_time, c.last_update_by, c.create_time, c.reload_flag, c.file_sync_flag  " +
          " from $(channel) c order by c.processing_order asc, c.channel_id                        ");

        putSql("selectNodeChannelsSql",
          "select c.channel_id, nc.node_id, nc.ignore_enabled, nc.suspend_enabled, c.processing_order,       "
        + "  c.max_batch_size, c.enabled, c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route, "
        + "  c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob, c.batch_algorithm,         "
        + "  nc.last_extract_time, c.extract_period_millis, c.data_loader_type,                              " +
        "    last_update_time, last_update_by, create_time, c.reload_flag, c.file_sync_flag                  "
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
         + "  extract_period_millis, data_loader_type, last_update_time, last_update_by,          "
         + "  create_time, reload_flag, file_sync_flag)                                           "
         + "  values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?, ?, ?, ?, ?, ?)                 ");

        putSql("updateChannelSql",
           "update $(channel) set processing_order=?, max_batch_size=?,                                          "
         + "  max_batch_to_send=?, max_data_to_route=?, use_old_data_to_route=?, use_row_data_to_route=?,        "
         + "  use_pk_data_to_route=?, contains_big_lob=?, enabled=?, batch_algorithm=?, extract_period_millis=?, "
         + "  data_loader_type=?, last_update_time=?, last_update_by=?, reload_flag=?, file_sync_flag=?          "
         + " where channel_id=?                                                                                  ");

        putSql("deleteNodeGroupLinkSql",
           "delete from $(node_group_link) where source_node_group_id=? and target_node_group_id=?   ");

        putSql("deleteNodeGroupSql", "delete from $(node_group) where node_group_id=?   ");

        putSql("deleteChannelSql", "delete from $(channel) where channel_id=?   ");

        putSql("deleteNodeChannelSql", "delete from $(node_channel_ctl) where channel_id=?   ");

        putSql("selectNodeGroupChannelWindowSql",
                "select node_group_id, channel_id, start_time, end_time, enabled                    "
              + "  from $(node_group_channel_wnd) where node_group_id=? and channel_id=?   ");

        putSql("insertNodeChannelControlSql", ""
                + "insert into $(node_channel_ctl) (node_id, channel_id,                         "
                + "  suspend_enabled, ignore_enabled,last_extract_time) values (?, ?, ?, ?, ?)   ");

        putSql("updateNodeChannelControlSql",
               "update $(node_channel_ctl) set                                                              "
             + "  suspend_enabled=?, ignore_enabled=?, last_extract_time=? where node_id=? and channel_id=? ");

        putSql("getRegistrationRedirectSql",
            "select registrant_external_id, registration_node_id from $(registration_redirect)");

        putSql("selectMaxChannelLastUpdateTime" ,"select max(last_update_time) from $(channel) where last_update_time is not null" );
        putSql("selectMaxNodeGroupLastUpdateTime" ,"select max(last_update_time) from $(node_group) where last_update_time is not null" );
        putSql("selectMaxNodeGroupLinkLastUpdateTime" ,"select max(last_update_time) from $(node_group_link) where last_update_time is not null" );

    }

}