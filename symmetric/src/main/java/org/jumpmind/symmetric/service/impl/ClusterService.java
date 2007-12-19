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

    private long lockTimeoutInMilliseconds;

    private String aquireLockSql;

    private String releaseLockSql;

    private String insertLockSql;

    private boolean lockDuringPurge = false;

    private boolean lockDuringPull = false;

    private boolean lockDuringPush = false;

    private boolean lockDuringHeartbeat = false;
    
    private boolean lockDuringSyncTriggers = false;
    
    private boolean lockDuringExtract = false;

    private INodeService nodeService;

    public void initLockTable() {
        initLockTableForNodes(nodeService.findNodesToPull());
        initLockTableForNodes(nodeService.findNodesToPushTo());
        initLockTable(LockAction.PURGE, COMMON_LOCK_ID);
        initLockTable(LockAction.SYNCTRIGGERS, COMMON_LOCK_ID);
    }

    private void initLockTableForNodes(List<Node> nodes) {
        for (Node node : nodes) {
            initLockTableForNode(node);
        }
    }

    public void initLockTableForNode(Node node) {
        initLockTable(LockAction.PULL, node.getNodeId());
        initLockTable(LockAction.PUSH, node.getNodeId());
        initLockTable(LockAction.HEARTBEAT, node.getNodeId());
    }
    
    private void initLockTable(LockAction action, String lockId) {
        try {            
            jdbcTemplate.update(insertLockSql, new Object[] { lockId, action.name() });
            logger.debug("Inserted into the node_lock table for " + lockId + ".");
        } catch (DataIntegrityViolationException ex) {
            logger.debug("Failed to insert to the node_lock table for " + lockId
                    + ".  Must be intialized already.");
        }        
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(LockAction action, Node node) {
        return lock(action, node.getNodeId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean lock(LockAction action) {
        return lock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(LockAction action) {
        unlock(action, COMMON_LOCK_ID);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void unlock(LockAction action, Node node) {
        unlock(action, node.getNodeId());
    }

    private boolean lock(LockAction action, String id) {
        if (isClusteringEnabled(action)) {
            Date timeout = DateUtils.add(new Date(), Calendar.MILLISECOND, (int) lockTimeoutInMilliseconds);
            return jdbcTemplate.update(aquireLockSql, new Object[] { getLockingServerId(), id, action.name(), timeout }) == 1;
        } else {
            return true;
        }
    }

    private String getLockingServerId() {
        return AppUtils.getServerId();
    }

    private void unlock(LockAction action, String id) {
        if (isClusteringEnabled(action)) {
            jdbcTemplate.update(releaseLockSql, new Object[] { id, action.name(), getLockingServerId() });
        }
    }

    private boolean isClusteringEnabled(LockAction action) {
        switch (action) {
        case PULL:
            return lockDuringPull;
        case PUSH:
            return lockDuringPush;
        case PURGE:
            return lockDuringPurge;
        case HEARTBEAT:
            return lockDuringHeartbeat;
        case SYNCTRIGGERS:
            return lockDuringSyncTriggers;
        case EXTRACT:
            return lockDuringExtract;
        case OTHER:
            return true;
        default:
            return false;
        }
    }

    public void setLockTimeoutInMilliseconds(long lockTimeoutInMilliseconds) {
        this.lockTimeoutInMilliseconds = lockTimeoutInMilliseconds;
    }

    public void setAquireLockSql(String aquireLockSql) {
        this.aquireLockSql = aquireLockSql;
    }

    public void setReleaseLockSql(String releaseLockSql) {
        this.releaseLockSql = releaseLockSql;
    }

    public void setInsertLockSql(String insertLockSql) {
        this.insertLockSql = insertLockSql;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setLockDuringPurge(boolean lockDuringPurge) {
        this.lockDuringPurge = lockDuringPurge;
    }

    public void setLockDuringPull(boolean lockDuringPull) {
        this.lockDuringPull = lockDuringPull;
    }

    public void setLockDuringPush(boolean lockDuringPush) {
        this.lockDuringPush = lockDuringPush;
    }

    public void setLockDuringHeartbeat(boolean lockDuringHeartbeat) {
        this.lockDuringHeartbeat = lockDuringHeartbeat;
    }

    public void setLockDuringSyncTriggers(boolean lockDuringSyncTriggers) {
        this.lockDuringSyncTriggers = lockDuringSyncTriggers;
    }

    public void setLockDuringExtract(boolean lockDuringExtract) {
        this.lockDuringExtract = lockDuringExtract;
    }

}