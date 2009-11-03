/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import static org.jumpmind.symmetric.service.LockActionConstants.HEARTBEAT;
import static org.jumpmind.symmetric.service.LockActionConstants.PULL;
import static org.jumpmind.symmetric.service.LockActionConstants.PURGE_INCOMING;
import static org.jumpmind.symmetric.service.LockActionConstants.PURGE_OUTGOING;
import static org.jumpmind.symmetric.service.LockActionConstants.PURGE_STATISTICS;
import static org.jumpmind.symmetric.service.LockActionConstants.PUSH;
import static org.jumpmind.symmetric.service.LockActionConstants.SYNCTRIGGERS;
import static org.jumpmind.symmetric.service.LockActionConstants.ROUTE;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public class ClusterService extends AbstractService implements IClusterService {

    protected static final String COMMON_LOCK_ID = "common";

    protected String serverId = AppUtils.getServerId();

    public void initLockTable() {
        initLockTable(LockActionConstants.ROUTE, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.PULL, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.PUSH, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.HEARTBEAT, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.PURGE_INCOMING, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.PURGE_OUTGOING, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.PURGE_STATISTICS, COMMON_LOCK_ID);
        initLockTable(LockActionConstants.SYNCTRIGGERS, COMMON_LOCK_ID);
    }

    public void initLockTableForNode(String action, final Node node) {
        initLockTable(action, node.getNodeId());
    }

    public void initLockTable(final String action, final String lockId) {
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
    public boolean lock(final String action, final Node node) {
        return lock(action, node.getNodeId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(final String action) {
        return lock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final String action) {
        unlock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final String action, final Node node) {
        unlock(action, node.getNodeId());
    }

    private boolean lock(final String action, final String id) {
        if (isClusteringEnabled(action)) {
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

    private void unlock(final String action, final String id) {
        if (isClusteringEnabled(action)) {
            jdbcTemplate.update(getSql("releaseLockSql"), new Object[] { id, action, serverId });
        }
    }

    private boolean isClusteringEnabled(final String action) {
        if (PULL.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PULL);
        } else if (PUSH.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PUSH);
        } else if (ROUTE.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_ROUTE);
        } else if (PURGE_INCOMING.equals(action) || PURGE_OUTGOING.equals(action) || PURGE_STATISTICS.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PURGE);
        } else if (HEARTBEAT.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_HEARTBEAT);
        } else if (SYNCTRIGGERS.equals(action)) {
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_SYNC_TRIGGERS);
        } else {
            return true;
        }
    }

}