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
 * under the License. 
 */
package org.jumpmind.symmetric.service.impl;

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
        initLockTable(ROUTE);
        initLockTable(PULL);
        initLockTable(PUSH);
        initLockTable(HEARTBEAT);
        initLockTable(PURGE_INCOMING);
        initLockTable(PURGE_OUTGOING);
        initLockTable(PURGE_STATISTICS);
        initLockTable(SYNCTRIGGERS);
        initLockTable(PURGE_DATA_GAPS);
    }

    public void initLockTable(final String action) {
        try {
            jdbcTemplate.update(getSql("insertLockSql"), new Object[] { action });
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
        if (isClusteringEnabled()) {
            final Date timeout = DateUtils.add(new Date(), Calendar.MILLISECOND,
                    (int) -parameterService.getLong(ParameterConstants.CLUSTER_LOCK_TIMEOUT_MS));
            return lock(action, timeout, new Date(), serverId);
        } else {
            return true;
        }
    }

    protected boolean lock(String action, Date timeToBreakLock, Date timeLockAquired,
            String serverId) {
        return jdbcTemplate.update(getSql("aquireLockSql"), new Object[] { serverId,
                timeLockAquired, action, timeToBreakLock, serverId }) == 1;
    }

    public Map<String, Lock> findLocks() {
        final Map<String, Lock> locks = new HashMap<String, Lock>();
        if (isClusteringEnabled()) {
            jdbcTemplate.query(getSql("findLocksSql"), new RowMapper<Lock>() {
                public Lock mapRow(ResultSet rs, int rowNum) throws SQLException {
                    Lock lock = new Lock();
                    lock.setLockAction(rs.getString(1));
                    lock.setLockingServerId(rs.getString(2));
                    lock.setLockTime(rs.getTimestamp(3));
                    lock.setLastLockingServerId(rs.getString(4));
                    lock.setLastLockTime(rs.getTimestamp(5));
                    locks.put(lock.getLockAction(), lock);
                    return lock;
                }
            });
        }
        return locks;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getServerId() {
        return serverId;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final String action) {
        if (isClusteringEnabled()) {
            if (!unlock(action, serverId)) {
                log.warn("ClusterUnlockFailed", action, serverId);
            }
        }
    }

    protected boolean unlock(String action, String serverId) {
        return jdbcTemplate.update(getSql("releaseLockSql"), new Object[] { serverId, action,
                serverId }) > 0;
    }

    public boolean isClusteringEnabled() {
        return parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED);
    }

    public void aquireInfiniteLock(String action) {
        if (isClusteringEnabled()) {
            Date futureTime = DateUtils.add(new Date(), Calendar.YEAR, 100);
            lock(action, new Date(), futureTime, Lock.STOPPED);
        }
    }

    public void clearInfiniteLock(String action) {
        Map<String, Lock> all = findLocks();
        Lock lock = all.get(action);
        if (lock != null && Lock.STOPPED.equals(lock.getLockingServerId())) {
            unlock(action, Lock.STOPPED);
        }
    }

}