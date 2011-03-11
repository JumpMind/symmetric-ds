/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import static org.jumpmind.symmetric.service.ClusterConstants.COMMON_LOCK_ID;
import static org.jumpmind.symmetric.service.ClusterConstants.HEARTBEAT;
import static org.jumpmind.symmetric.service.ClusterConstants.PULL;
import static org.jumpmind.symmetric.service.ClusterConstants.PURGE_DATA_GAPS;
import static org.jumpmind.symmetric.service.ClusterConstants.PURGE_INCOMING;
import static org.jumpmind.symmetric.service.ClusterConstants.PURGE_OUTGOING;
import static org.jumpmind.symmetric.service.ClusterConstants.PURGE_STATISTICS;
import static org.jumpmind.symmetric.service.ClusterConstants.PUSH;
import static org.jumpmind.symmetric.service.ClusterConstants.ROUTE;
import static org.jumpmind.symmetric.service.ClusterConstants.SYNCTRIGGERS;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * @see IClusterService
 */
public class ClusterService extends AbstractService implements IClusterService {

    protected String serverId = AppUtils.getServerId();

    public void initLockTable() {
        initLockTable(ROUTE, COMMON_LOCK_ID);
        initLockTable(PULL, COMMON_LOCK_ID);
        initLockTable(PUSH, COMMON_LOCK_ID);
        initLockTable(HEARTBEAT, COMMON_LOCK_ID);
        initLockTable(PURGE_INCOMING, COMMON_LOCK_ID);
        initLockTable(PURGE_OUTGOING, COMMON_LOCK_ID);
        initLockTable(PURGE_STATISTICS, COMMON_LOCK_ID);
        initLockTable(SYNCTRIGGERS, COMMON_LOCK_ID);
        initLockTable(PURGE_DATA_GAPS, COMMON_LOCK_ID);
    }
    
    public void initLockTable(final String action) {
        initLockTable(action, COMMON_LOCK_ID);
    }

    protected void initLockTable(final String action, final String lockId) {
        try {
            jdbcTemplate.update(getSql("insertLockSql"), new Object[] { lockId, action });
            log.debug("LockInserted", action);

        } catch (final DataIntegrityViolationException ex) {
            log.debug("LockInsertFailed", action);
        }
    }

    public void clearAllLocks() {
        jdbcTemplate.update(getSql("clearAllLocksSql"));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(final String action) {
        return lock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final String action) {
        unlock(action, COMMON_LOCK_ID);
    }
    
    public Map<String,Lock> findLocks() {
        final Map<String, Lock> locks = new HashMap<String, Lock>();
        jdbcTemplate.query(getSql("findLocksSql"), new RowMapper<Lock>() {
            public Lock mapRow(ResultSet rs, int rowNum) throws SQLException {
                Lock lock = new Lock();
                lock.setLockId(rs.getString(1));
                lock.setLockAction(rs.getString(2));
                lock.setLockingServerId(rs.getString(3));
                lock.setLockTime(rs.getTimestamp(4));
                lock.setLastLockingServerId(rs.getString(5));
                lock.setLastLockTime(rs.getTimestamp(6));
                locks.put(lock.getLockAction(), lock);
                return lock;
            }
        });
        return locks;
    }

    protected boolean lock(final String action, final String id) {
        if (isClusteringEnabled()) {
            final Date timeout = DateUtils.add(new Date(), Calendar.MILLISECOND, (int) -parameterService
                    .getLong(ParameterConstants.CLUSTER_LOCK_TIMEOUT_MS));
            return jdbcTemplate.update(getSql("aquireLockSql"),
                    new Object[] { serverId, id, action, timeout, serverId }) == 1;
        } else {
            return true;
        }
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getServerId() {
        return serverId;
    }

    protected void unlock(final String action, final String id) {
        if (isClusteringEnabled()) {
            int count = jdbcTemplate.update(getSql("releaseLockSql"), new Object[] { serverId, id, action, serverId });
            if (count == 0) {
                log.warn("ClusterUnlockFailed", id, action, serverId);
            }
        }
    }

    public boolean isClusteringEnabled() {
        return parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED);
    }

}