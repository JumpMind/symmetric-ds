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

package org.jumpmind.symmetric.model;

public class NodeChannel extends Channel {

    private static final long serialVersionUID = -2493052366767513160L;

    String nodeId;

    private boolean ignored;

    private boolean suspended;

    public boolean isIgnored() {
        return ignored;
    }

    public void setIgnored(boolean ignore) {
        this.ignored = ignore;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public void setSuspended(boolean suspend) {
        this.suspended = suspend;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
