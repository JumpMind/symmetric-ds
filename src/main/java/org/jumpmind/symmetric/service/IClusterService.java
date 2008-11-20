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

package org.jumpmind.symmetric.service;

import org.jumpmind.symmetric.model.Node;

public interface IClusterService {

    public void initLockTable();

    public void initLockTableForNode(Node node);

    public void initLockTable(LockAction action, String lockId);

    public boolean lock(LockAction action, Node node);

    public boolean lock(LockAction action);

    public void unlock(LockAction action);

    public void unlock(final LockAction action, final Node node);

    public void clearAllLocks();

}
