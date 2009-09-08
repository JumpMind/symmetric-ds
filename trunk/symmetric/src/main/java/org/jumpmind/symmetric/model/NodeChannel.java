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

public class NodeChannel {

    private static final long serialVersionUID = -2493052366767513160L;
    private Channel channel;
    private NodeChannelControl nodeChannelControl;

    public NodeChannel() {
        channel = new Channel();
        nodeChannelControl = new NodeChannelControl();
    }

    public NodeChannel(String channelId) {
        channel = new Channel();
        nodeChannelControl = new NodeChannelControl();
        channel.setId(channelId);
    }

    public String getId() {
        return channel.getId();
    }

    public int getMaxBatchSize() {
        return channel.getMaxBatchSize();
    }

    public void setMaxBatchSize(int maxBatchSize) {
        channel.setMaxBatchSize(maxBatchSize);
    }

    public int getMaxBatchToSend() {
        return channel.getMaxBatchToSend();
    }

    public void setMaxBatchToSend(int maxBatchToSend) {
        channel.setMaxBatchToSend(maxBatchToSend);
    }

    public int getProcessingOrder() {
        return channel.getProcessingOrder();
    }

    public String getBatchAlgorithm() {
        return channel.getBatchAlgorithm();
    }

    public void setBatchAlgorithm(String batchAlgorithm) {
        channel.setBatchAlgorithm(batchAlgorithm);
    }

    public void setEnabled(boolean enabled) {
        channel.setEnabled(enabled);
    }

    public boolean isEnabled() {
        return channel.isEnabled();
    }

    public boolean isSuspended() {
        return nodeChannelControl.isSuspended();
    }

    public boolean isIgnored() {
        return nodeChannelControl.isIgnored();
    }

    public String getNodeId() {
        return nodeChannelControl.getNodeId();
    }

    public void setNodeId(String nodeId) {
        nodeChannelControl.setNodeId(nodeId);
    }

    public void setLastExtractedTime(Date lastExtractedTime) {
        nodeChannelControl.setLastExtractedTime(lastExtractedTime);
    }

    public Date getLastExtractedTime() {
        return nodeChannelControl.getLastExtractedTime();
    }

    public void setIgnored(boolean ignored) {
        nodeChannelControl.setIgnored(ignored);
    }

    public void setProcessingOrder(int priority) {
        channel.setProcessingOrder(priority);
    }

    public void setId(String id) {
        channel.setId(id);
    }

    public void setSuspended(boolean suspended) {
        nodeChannelControl.setSuspended(suspended);
    }

    public Channel getChannel() {
        return channel;
    }

    public NodeChannelControl getNodeChannelControl() {
        return nodeChannelControl;
    }

}
