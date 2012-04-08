package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class RegistrationServiceSqlMap extends AbstractSqlMap {

    public RegistrationServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("findNodeToRegisterSql",
                ""
                        + "select min(c.node_id) from $(node) c inner join                                "
                        + "  $(node_security) s on c.node_id = s.node_id where s.registration_enabled =   "
                        + "  1 and c.node_group_id = ? and c.external_id = ?                                    ");

        putSql("registerNodeSql",
                ""
                        + "update $(node) set sync_enabled = 1, heartbeat_time = current_timestamp, sync_url = ?, schema_version = ?,   "
                        + "  database_type = ?, database_version = ?, symmetric_version = ? where node_id = ?                                 ");

        putSql("registerNodeSecuritySql",
                ""
                        + "update $(node_security) set registration_enabled = 0, registration_time =   "
                        + "  current_timestamp where node_id = ?                                             ");

        putSql("reopenRegistrationSql",
                ""
                        + "update $(node_security) set node_password = ?, registration_enabled = 1,   "
                        + "  registration_time = null where node_id = ?                                     ");

        putSql("openRegistrationNodeSecuritySql", ""
                + "insert into $(node_security) (node_id, node_password,       "
                + "  registration_enabled, created_at_node_id) values (?, ?, 1, ?)   ");

        putSql("getRegistrationRedirectUrlSql",
                ""
                        + "select sync_url from $(node) n inner join $(registration_redirect) r on n.node_id=r.registration_node_id where r.registrant_external_id=?   ");

        putSql("insertRegistrationRedirectUrlSql",
                ""
                        + "insert into $(registration_redirect) (registration_node_id, registrant_external_id) values (?, ?)   ");

        putSql("updateRegistrationRedirectUrlSql",
                ""
                        + "update $(registration_redirect) set registration_node_id=? where registrant_external_id=?   ");

        putSql("insertRegistrationRequestSql",
                ""
                        + "insert into $(registration_request)                                         "
                        + "  (last_update_by, last_update_time, attempt_count, registered_node_id, status,   "
                        + "  node_group_id, external_id, ip_address, host_name, create_time)                 "
                        + "  values (?,?,1,?,?,?,?,?,?,current_timestamp)                                    ");

        putSql("updateRegistrationRequestSql",
                ""
                        + "update $(registration_request)                                                                    "
                        + "  set                                                                                                   "
                        + "  last_update_by=?, last_update_time=?, attempt_count=attempt_count+1, registered_node_id=?, status=?   "
                        + "  where                                                                                                 "
                        + "  node_group_id=? and external_id=? and ip_address=? and host_name=? and status=?                       ");

        putSql("selectRegistrationRequestSql",
                ""
                        + "select node_group_id, external_id, status, host_name, ip_address,                    "
                        + "  attempt_count, registered_node_id, create_time, last_update_by, last_update_time   "
                        + "  from $(registration_request)                                                 "
                        + "  where status=?                                                                     ");

        putSql("deleteRegistrationRequestSql",
                "delete from $(registration_request) where node_group_id=? and external_id=? and ip_address=? and host_name=? and status=?");

    }

}