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

public class DataExtractorServiceSqlMap extends AbstractSqlMap {

    public DataExtractorServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        
        // @formatter:off
        putSql("selectNodeIdsForExtractSql", "select node_id, queue from $(extract_request) where status=? group by node_id, queue");
        
        putSql("selectExtractRequestForNodeSql", "select * from $(extract_request) where node_id=? and queue=? and status=? order by request_id");
        
        putSql("insertExtractRequestSql", "insert into $(extract_request) (request_id, node_id, queue, status, start_batch_id, end_batch_id, trigger_id, router_id, last_update_time, create_time) values(?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)");
        
        putSql("updateExtractRequestStatus", "update $(extract_request) set status=?, last_update_time=current_timestamp where request_id=?");
        
        putSql("resetExtractRequestStatus", "update $(extract_request) set status=?, last_update_time= current_timestamp where start_batch_id <= ? and end_batch_id >= ? and node_id=?");
    }

}
