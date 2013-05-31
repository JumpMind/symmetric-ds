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

public class ClusterServiceSqlMap extends AbstractSqlMap {

    public ClusterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        putSql("aquireLockSql",
                ""
                        + "update $(lock) set locking_server_id=?, lock_time=? where                   "
                        + "  lock_action=? and (lock_time is null or lock_time < ? or locking_server_id=?)   ");

        putSql("releaseLockSql",
                ""
                        + "update $(lock) set locking_server_id=null, lock_time=null, last_lock_time=current_timestamp, last_locking_server_id=?   "
                        + "  where lock_action=? and locking_server_id=?                                                                                 ");

        putSql("insertLockSql", "" + "insert into $(lock) (lock_action) values(?)   ");
        
        putSql("findLocksSql",
                ""
                        + "select lock_action, locking_server_id, lock_time, last_locking_server_id, last_lock_time   "
                        + "  from $(lock)                                                                       ");

    }

}