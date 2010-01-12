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
package org.jumpmind.symmetric.io;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Node;

/**
 * This is an extension point that is called when the current instance has detected it cannot sync with another
 * {@link Node}.
 */
public interface IOfflineListener extends IExtensionPoint {

    /**
     * Called when the remote node is unreachable.
     */
    public void offline(Node remoteNode);

    /**
     * Called when this node is rejected because of a password mismatch.
     */
    public void notAuthenticated(Node remoteNode);

    /**
     * Called when this node has been rejected because the remote node is currently too busy to handle the sync request.
     */
    public void busy(Node remoteNode);

}
