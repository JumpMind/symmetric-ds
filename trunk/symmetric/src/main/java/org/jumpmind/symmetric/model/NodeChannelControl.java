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

import java.util.Date;

public class NodeChannelControl {

    private static final long serialVersionUID = -2493052366767513160L;

    private String nodeId = null;
    
    private String channelId = null;

    private boolean ignoreEnabled = false;

    private boolean suspendEnabled = false;

    private Date lastExtractTime = null;
    
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isIgnoreEnabled() {
        return ignoreEnabled;
    }

    public void setIgnoreEnabled(boolean ignored) {
        this.ignoreEnabled = ignored;
    }

    public boolean isSuspendEnabled() {
        return suspendEnabled;
    }

    public void setSuspendEnabled(boolean suspended) {
        this.suspendEnabled = suspended;
    }

    public Date getLastExtractTime() {
        return lastExtractTime;
    }

    public void setLastExtractTime(Date lastExtractedTime) {
        this.lastExtractTime = lastExtractedTime;
    }

}
