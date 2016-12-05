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
package org.jumpmind.symmetric.job;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.service.impl.AbstractSqlMap;

public class JobManagerSqlMap extends AbstractSqlMap {

    public JobManagerSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("loadCustomJobs",
                "select * from $(job) order by job_name");
        
        putSql("insertJobSql", "insert into $(job) (external_id, node_group_id, description, job_type, schedule, startup_type, schedule_type, job_expression, create_by, create_time, "
                + "last_update_by, last_update_time, job_name) " +
                "values ('ALL', 'ALL', ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?, current_timestamp, ?)");
        
        putSql("updateJobSql", "update $(job) set description = ?, job_type = ?, " +
                "schedule = ?, startup_type = ?, schedule_type = ?, job_expression = ?, create_by = ?, last_update_by = ?, " +
                "last_update_time = current_timestamp where job_name = ?");        
    }

}
