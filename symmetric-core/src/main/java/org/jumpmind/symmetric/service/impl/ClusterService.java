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

import static org.jumpmind.symmetric.service.ClusterConstants.*;


import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.sql.ConcurrencySqlException;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;

/**
 * @see IClusterService
 */
public class ClusterService extends AbstractService implements IClusterService {

    private static final String[] actions = new String[] { ROUTE, PULL, PUSH, HEARTBEAT, PURGE_INCOMING, PURGE_OUTGOING,
            PURGE_STATISTICS, SYNC_TRIGGERS, PURGE_DATA_GAPS, STAGE_MANAGEMENT, WATCHDOG, STATISTICS, FILE_SYNC_PULL,
            FILE_SYNC_PUSH, FILE_SYNC_TRACKER, INITIAL_LOAD_EXTRACT, OFFLINE_PUSH, OFFLINE_PULL, MONITOR, SYNC_CONFIG };
    
    private static final String[] sharedActions = new String[] { FILE_SYNC_SHARED };

    private String serverId = null;
    
    private Map<String, Lock> lockCache = new ConcurrentHashMap<String, Lock>();

    public ClusterService(IParameterService parameterService, ISymmetricDialect dialect) {
        super(parameterService, dialect);
        setSqlMap(new ClusterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
        initCache();
    }

    public void init() {
        if (isClusteringEnabled()) {
            sqlTemplate.update(getSql("initLockSql"), new Object[] { getServerId() });

            Map<String, Lock> allLocks = findLocks();

            for (String action : actions) {
                if (allLocks.get(action) == null) {
                    initLockTable(action, TYPE_CLUSTER);
                }
            }
            
            for (String action : sharedActions) {
                if (allLocks.get(action) == null) {
                    initLockTable(action, TYPE_SHARED);
                }
            }
        }
    }
    
    @Override
    public synchronized void persistToTableForSnapshot() {
        sqlTemplate.update(getSql("deleteSql"));
        Collection<Lock> values = lockCache.values();
        for (Lock lock : values) {
            insertLock(lock);
        }
    }
    
    protected void insertLock(Lock lock) {
        sqlTemplate.update(getSql("insertCompleteLockSql"), lock.getLockAction(), lock.getLockType(), lock.getLockingServerId(), lock.getLockTime(), lock.getSharedCount(), lock.isSharedEnable() ? 1 : 0, lock.getLastLockTime(), lock.getLastLockingServerId());
    }


    protected void initLockTable(final String action) {
        initLockTable(action, TYPE_CLUSTER);
    }
    
    protected void initLockTable(final String action, final String lockType) {
        try {
            sqlTemplate.update(getSql("insertLockSql"), new Object[] { action, lockType });
            log.debug("Inserted into the LOCK table for {}, {}", action, lockType);
        } catch (UniqueKeyException ex) {
            log.debug("Failed to insert to the LOCK table for {}, {}.  Must be initialized already.",
                    action, lockType);
        }
    }

    protected void initCache() {
        lockCache .clear();
        for (String action : actions) {
            Lock lock = new Lock();
            lock.setLockAction(action);
            lock.setLockType(TYPE_CLUSTER);
            lockCache.put(action, lock);
        }
        for (String action : sharedActions) {
            Lock lock = new Lock();
            lock.setLockAction(action);
            lock.setLockType(TYPE_SHARED);
            lockCache.put(action, lock);
        }
    }

    public void clearAllLocks() {
        if (isClusteringEnabled()) {
            sqlTemplate.update(getSql("initLockSql"), new Object[] { getServerId() });
        } else {
            initCache();
        }
    }

    public boolean lock(final String action, final String lockType) {
        if (lockType.equals(TYPE_CLUSTER)) {
            return lock(action);
        } else if (lockType.equals(TYPE_SHARED)) {
            return lockShared(action);
        } else if (lockType.equals(TYPE_EXCLUSIVE)) {
            return lockExclusive(action);
        } else {
            throw new UnsupportedOperationException("Lock type of " + lockType + " is not supported");
        }
    }

    public boolean lock(final String action, final String lockType, long waitMillis) {
        if (lockType.equals(TYPE_SHARED) || lockType.equals(TYPE_EXCLUSIVE)) {
            return lockWait(action, lockType, waitMillis);
        } else {
            throw new UnsupportedOperationException("Lock type of " + lockType + " is not supported");
        }
    }

    public boolean lock(final String action) {
        final Date timeout = DateUtils.addMilliseconds(new Date(), 
                (int) -parameterService.getLong(ParameterConstants.CLUSTER_LOCK_TIMEOUT_MS));
        return lockCluster(action, timeout, new Date(), getServerId());
    }

    protected boolean lockCluster(String action, Date timeToBreakLock, Date timeLockAcquired,
            String serverId) {
        if (isClusteringEnabled()) {
            try {
                
                boolean lockAcquired = sqlTemplate.update(getSql("acquireClusterLockSql"), new Object[] { serverId,
                        timeLockAcquired, action, TYPE_CLUSTER, timeToBreakLock, serverId }) == 1;
                if (lockAcquired) {
                    updateCacheLockTime(action, timeLockAcquired);
                }
                return lockAcquired;
            } catch (ConcurrencySqlException ex) {
                log.debug("Ignoring concurrency error and reporting that we failed to get the cluster lock: {}", ex.getMessage());
            }
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if (lock.getLockType().equals(TYPE_CLUSTER) && (lock.getLockTime() == null 
                            || lock.getLockTime().before(timeToBreakLock) || serverId.equals(lock.getLockingServerId()))) {
                        lock.setLockingServerId(serverId);
                        lock.setLockTime(timeLockAcquired);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected void updateCacheLockTime(String action, Date timeLockAcquired) {
        Lock lock = lockCache.get(action);
        if (lock != null) {
            synchronized (lock) {
                lock.setLockTime(timeLockAcquired);
            }
        }
    }

    protected boolean lockShared(final String action) {
        final Date timeout = DateUtils.addMilliseconds(new Date(),
                (int) -parameterService.getLong(ParameterConstants.LOCK_TIMEOUT_MS));    
        if (isClusteringEnabled()) {
            return sqlTemplate.update(getSql("acquireSharedLockSql"), new Object[] {
                    TYPE_SHARED, getServerId(), new Date(), action, TYPE_SHARED, timeout }) == 1;
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if ((lock.getLockType().equals(TYPE_SHARED) || lock.getLockTime() == null || lock.getLockTime().before(timeout))
                            && (lock.isSharedEnable() || lock.getSharedCount() == 0)) {
                        lock.setLockType(TYPE_SHARED);
                        lock.setLockingServerId(getServerId());
                        lock.setLockTime(new Date());
                        if (lock.getSharedCount() == 0) {
                            lock.setSharedEnable(true);
                        }
                        lock.setSharedCount(lock.getSharedCount() + 1);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected boolean lockExclusive(final String action) {
        final Date timeout = DateUtils.addMilliseconds(new Date(),
                (int) -parameterService.getLong(ParameterConstants.LOCK_TIMEOUT_MS));    
        if (isClusteringEnabled()) {
            return sqlTemplate.update(getSql("acquireExclusiveLockSql"), new Object[] {
                    TYPE_EXCLUSIVE, getServerId(), new Date(), action, TYPE_SHARED, timeout }) == 1;
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if ((lock.getLockType().equals(TYPE_SHARED) && lock.getSharedCount() == 0) ||
                            lock.getLockTime() == null || lock.getLockTime().before(timeout)) {
                        lock.setLockType(TYPE_EXCLUSIVE);
                        lock.setLockingServerId(getServerId());
                        lock.setLockTime(new Date());
                        lock.setSharedCount(0);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected boolean lockWait(final String action, final String lockType, long waitMillis) {
        boolean isLocked = false;
        long endTime = System.currentTimeMillis() + waitMillis;
        long sleepMillis = parameterService.getLong(ParameterConstants.LOCK_WAIT_RETRY_MILLIS);
        do {
            if (lockType.equals(TYPE_SHARED)) {
                isLocked = lockShared(action);
            } else if (lockType.equals(TYPE_EXCLUSIVE)) {
                isLocked = lockExclusive(action);
                if (!isLocked) {
                    if (isClusteringEnabled()) {
                        sqlTemplate.update(getSql("disableSharedLockSql"), new Object[] { action, TYPE_SHARED });
                    } else {
                        Lock lock = lockCache.get(action);
                        if (lock != null) {
                            synchronized (lock) {
                                lock.setSharedEnable(false);
                            }
                        }
                    }
                }
            }
            if (isLocked) {
                break;
            }
            AppUtils.sleep(sleepMillis);
        } while (waitMillis == 0 || System.currentTimeMillis() < endTime);
        return isLocked;
    }

    public Map<String, Lock> findLocks() {
        if (isClusteringEnabled()) {
            final Map<String, Lock> locks = new HashMap<String, Lock>();
            sqlTemplate.query(getSql("findLocksSql"), new ISqlRowMapper<Lock>() {
                public Lock mapRow(Row rs) {
                    Lock lock = new Lock();
                    lock.setLockAction(rs.getString("lock_action"));
                    lock.setLockType(rs.getString("lock_type"));
                    lock.setLockingServerId(rs.getString("locking_server_id"));
                    lock.setLockTime(rs.getDateTime("lock_time"));
                    lock.setSharedCount(rs.getInt("shared_count"));
                    lock.setSharedEnable(rs.getBoolean("shared_enable"));
                    lock.setLastLockingServerId(rs.getString("last_locking_server_id"));
                    lock.setLastLockTime(rs.getDateTime("last_lock_time"));
                    locks.put(lock.getLockAction(), lock);
                    return lock;
                }
            });
            return locks;
        } else {
            return lockCache;
        }
    }

    /**
     * Get a unique identifier that represents the JVM instance this server is
     * currently running in.
     */
    public String getServerId() {
        if (StringUtils.isBlank(serverId)) {
            serverId = parameterService.getString(ParameterConstants.CLUSTER_SERVER_ID);
            if (StringUtils.isBlank(serverId)) {
                serverId = System.getProperty(SystemConstants.SYSPROP_CLUSTER_SERVER_ID, null);
            }

            if (StringUtils.isBlank(serverId)) {
                // JBoss uses this system property to identify a server in a
                // cluster
                serverId = System.getProperty("bind.address", null);
            }

            if (StringUtils.isBlank(serverId)) {
                // JBoss uses this system property to identify a server in a
                // cluster
                serverId = System.getProperty("jboss.bind.address", null);
            }

            if (StringUtils.isBlank(serverId)) {
                try {
                    serverId = AppUtils.getHostName();
                } catch (Exception ex) {
                    serverId = "unknown";
                }
            }
            
            log.info("This node picked a server id of {}", serverId);
        }
        return serverId;
    }

    public void unlock(final String action, final String lockType) {
        if (lockType.equals(TYPE_CLUSTER)) {
            unlock(action);
        } else if (lockType.equals(TYPE_SHARED)) {
            unlockShared(action);
        } else if (lockType.equals(TYPE_EXCLUSIVE)) {
            unlockExclusive(action);
        } else {
            throw new UnsupportedOperationException("Lock type of " + lockType + " is not supported");
        }
    }

    public void unlock(final String action) {
        if (!unlockCluster(action, getServerId())) {
            log.warn("Failed to release lock for action:{} server:{}", action, getServerId());
        }
    }

    protected boolean unlockCluster(String action, String serverId) {
        if (isClusteringEnabled()) {
            updateCacheLockTime(action, null);
            return sqlTemplate.update(getSql("releaseClusterLockSql"), new Object[] { action,
                    TYPE_CLUSTER, serverId }) > 0;
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if (lock.getLockType().equals(TYPE_CLUSTER) && serverId.equals(lock.getLockingServerId())) {
                        lock.setLastLockingServerId(lock.getLockingServerId());
                        lock.setLockingServerId(null);
                        lock.setLastLockTime(lock.getLockTime());
                        lock.setLockTime(null);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected boolean unlockShared(final String action) {
        if (isClusteringEnabled()) {
            return sqlTemplate.update(getSql("releaseSharedLockSql"), new Object[] {
                    action, TYPE_SHARED }) == 1;
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if (lock.getLockType().equals(TYPE_SHARED)) {
                        lock.setLastLockTime(lock.getLockTime());
                        lock.setLastLockingServerId(lock.getLockingServerId());
                        if (lock.getSharedCount() == 1) {
                            lock.setSharedEnable(false);
                            lock.setLockingServerId(null);
                            lock.setLockTime(null);
                        }
                        if (lock.getSharedCount() > 1) {
                            lock.setSharedCount(lock.getSharedCount() - 1);
                        } else {
                            lock.setSharedCount(0);
                        }
                        return true;
                    }
                }
            }
        }
        return false;
    }

    protected boolean unlockExclusive(final String action) {
        if (isClusteringEnabled()) {
            return sqlTemplate.update(getSql("releaseExclusiveLockSql"), new Object[] {
                    action, TYPE_EXCLUSIVE }) == 1;
        } else {
            Lock lock = lockCache.get(action);
            if (lock != null) {
                synchronized (lock) {
                    if (lock.getLockType().equals(TYPE_EXCLUSIVE)) {
                        lock.setLastLockingServerId(lock.getLockingServerId());
                        lock.setLockingServerId(null);
                        lock.setLastLockTime(lock.getLockTime());
                        lock.setLockTime(null);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean isClusteringEnabled() {
        return parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED);
    }

    public boolean isInfiniteLocked(String action) {
        Map<String, Lock> locks = findLocks();
        Lock lock = locks.get(action);
        if (lock != null && lock.getLockTime() != null && new Date().before(lock.getLockTime())
                && Lock.STOPPED.equals(lock.getLockingServerId())) {
            return true;
        } else {
            return false;
        }
    }

    public void aquireInfiniteLock(String action) {
        if (isClusteringEnabled()) {
            int tries = 600;
            Date futureTime = DateUtils.addYears(new Date(), 100);
            while (tries > 0) {
                if (!lockCluster(action, new Date(), futureTime, Lock.STOPPED)) {
                    AppUtils.sleep(50);
                    tries--;
                } else {
                    tries = 0;
                }
            }
        }
    }

    public void clearInfiniteLock(String action) {
        Map<String, Lock> all = findLocks();
        Lock lock = all.get(action);
        if (lock != null && Lock.STOPPED.equals(lock.getLockingServerId())) {
            if (isClusteringEnabled()) {
                sqlTemplate.update(getSql("resetClusterLockSql"), new Object[] { action, TYPE_CLUSTER, Lock.STOPPED});
            } else if (lock.getLockType().equals(TYPE_CLUSTER) && Lock.STOPPED.equals(lock.getLockingServerId())) {
                lock.setLastLockingServerId(null);
                lock.setLockingServerId(null);
                lock.setLastLockTime(null);
                lock.setLockTime(null);
            }
        }
    }

    @Override
    public boolean refreshLock(String action) {
        if (isLockRefreshNeeded(action)) {
            return lock(action); 
        }
        return true;
    }
    
    protected boolean isLockRefreshNeeded(String action) {
        if (isClusteringEnabled()) {
            Lock lock = lockCache.get(action);
            long clusterLockRefreshMs = this.parameterService.getLong(ParameterConstants.CLUSTER_LOCK_REFRESH_MS);
            long refreshTime = new Date().getTime() - clusterLockRefreshMs;
            
            if (lock != null && lock.getLockTime() != null && lock.getLockTime().getTime() < refreshTime) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

}
