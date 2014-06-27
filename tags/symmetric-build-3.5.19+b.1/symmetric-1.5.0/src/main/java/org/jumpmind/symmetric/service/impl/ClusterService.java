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

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.LockAction;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

public class ClusterService extends AbstractService implements IClusterService {

    protected static final Log logger = LogFactory.getLog(ClusterService.class);

    protected static final String COMMON_LOCK_ID = "common";

    private INodeService nodeService;

    public void initLockTable() {
        initLockTableForNodes(nodeService.findNodesToPull());
        initLockTableForNodes(nodeService.findNodesToPushTo());
        initLockTable(LockAction.PURGE_INCOMING, COMMON_LOCK_ID);
        initLockTable(LockAction.PURGE_OUTGOING, COMMON_LOCK_ID);
        initLockTable(LockAction.PURGE_STATISTICS, COMMON_LOCK_ID);
        initLockTable(LockAction.SYNCTRIGGERS, COMMON_LOCK_ID);
    }

    private void initLockTableForNodes(final List<Node> nodes) {
        for (final Node node : nodes) {
            initLockTableForNode(node);
        }
    }

    public void initLockTableForNode(final Node node) {
        initLockTable(LockAction.PULL, node.getNodeId());
        initLockTable(LockAction.PUSH, node.getNodeId());
        initLockTable(LockAction.HEARTBEAT, node.getNodeId());
    }

    public void initLockTable(final LockAction action, final String lockId) {
        try {
            jdbcTemplate.update(getSql("insertLockSql"), new Object[] { lockId, action.name() });
            logger.debug("Inserted into the node_lock table for " + lockId + ".");
        } catch (final DataIntegrityViolationException ex) {
            logger.debug("Failed to insert to the node_lock table for " + lockId + ".  Must be intialized already.");
        }
    }

    public void clearAllLocks() {
        jdbcTemplate.update(getSql("clearAllLocksSql"));
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(final LockAction action, final Node node) {
        return lock(action, node.getNodeId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(final LockAction action) {
        return lock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final LockAction action) {
        unlock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(final LockAction action, final Node node) {
        unlock(action, node.getNodeId());
    }

    private boolean lock(final LockAction action, final String id) {
        if (isClusteringEnabled(action)) {
            final Date timeout = DateUtils.add(new Date(), Calendar.MILLISECOND, (int) -parameterService
                    .getLong(ParameterConstants.CLUSTER_LOCK_TIMEOUT_MS));
            return jdbcTemplate.update(getSql("aquireLockSql"), new Object[] { getLockingServerId(), id, action.name(),
                    timeout }) == 1;
        } else {
            return true;
        }
    }

    private String getLockingServerId() {
        return AppUtils.getServerId();
    }

    private void unlock(final LockAction action, final String id) {
        if (isClusteringEnabled(action)) {
            jdbcTemplate.update(getSql("releaseLockSql"), new Object[] { id, action.name(), getLockingServerId() });
        }
    }

    private boolean isClusteringEnabled(final LockAction action) {
        switch (action) {
        case PULL:
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PULL);
        case PUSH:
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PUSH);
        case PURGE_INCOMING:
        case PURGE_OUTGOING:
        case PURGE_STATISTICS:
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_PURGE);
        case HEARTBEAT:
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_HEARTBEAT);
        case SYNCTRIGGERS:
            return parameterService.is(ParameterConstants.CLUSTER_LOCK_DURING_SYNC_TRIGGERS);
        case OTHER:
            return true;
        default:
            return false;
        }
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}