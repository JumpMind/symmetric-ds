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

        putSql("acquireClusterLockSql",
            "update $(lock) set locking_server_id=?, lock_time=? " +
            "where lock_action=? and lock_type=? and (lock_time is null or lock_time < ? or locking_server_id=?)");

        putSql("acquireSharedLockSql",
            "update $(lock) set lock_type=?, locking_server_id=?, lock_time=?, " +
            "shared_enable=(case when shared_count = 0 then 1 else shared_enable end), shared_count=shared_count+1 " +
            "where lock_action=? and (lock_type=? or lock_time is null or lock_time < ?) " +
            "and (shared_enable = 1 or shared_count = 0)");

        putSql("disableSharedLockSql",
            "update $(lock) set shared_enable=0 where lock_action=? and lock_type=?");

        putSql("acquireExclusiveLockSql",
            "update $(lock) set lock_type=?, locking_server_id=?, lock_time=?, shared_count=0 " +
            "where lock_action=? and ((lock_type=? and shared_count = 0) or lock_time is null or lock_time < ?)");

        putSql("releaseClusterLockSql",
            "update $(lock) set last_locking_server_id=locking_server_id, locking_server_id=null, last_lock_time=lock_time, lock_time=null " +
            "where lock_action=? and lock_type=? and locking_server_id=?");

        putSql("resetClusterLockSql",
                "update $(lock) set last_locking_server_id=null, locking_server_id=null, last_lock_time=null, lock_time=null " +
                "where lock_action=? and lock_type=? and locking_server_id=?");

        putSql("releaseSharedLockSql",
            "update $(lock) set last_lock_time=lock_time, last_locking_server_id=locking_server_id, " +
            "shared_enable=(case when shared_count = 1 then 0 else shared_enable end), " +
            "locking_server_id = (case when shared_count = 1 then null else locking_server_id end), " +
            "lock_time = (case when shared_count = 1 then null else lock_time end), " +
            "shared_count=(case when shared_count > 1 then shared_count-1 else 0 end) " +
            "where lock_action=? and lock_type=?");

        putSql("releaseExclusiveLockSql",
            "update $(lock) set last_locking_server_id=locking_server_id, locking_server_id=null, last_lock_time=lock_time, lock_time=null " +
            "where lock_action=? and lock_type=?");

        putSql("initLockSql", 
            "update $(lock) set last_locking_server_id=locking_server_id, locking_server_id=null, last_lock_time=lock_time, " +
            "lock_time=null, shared_count=0, shared_enable=0 " +
            "where locking_server_id=?");

        putSql("insertLockSql", "insert into $(lock) (lock_action, lock_type) values(?,?)");

        putSql("findLocksSql",
            "select lock_action, lock_type, locking_server_id, lock_time, shared_count, shared_enable, " +
            "last_locking_server_id, last_lock_time " +
            "from $(lock)");

    }

}