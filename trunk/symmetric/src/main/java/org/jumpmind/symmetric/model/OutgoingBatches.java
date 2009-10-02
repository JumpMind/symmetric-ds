/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Mark Hanes <eegeek@users.sourceforge.net>
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OutgoingBatches {

    List<OutgoingBatch> batches = new ArrayList<OutgoingBatch>();
    Set<NodeChannel> activeChannels = new HashSet<NodeChannel>();
    Set<String> activeChannelIds = new HashSet<String>();

    public OutgoingBatches(List<OutgoingBatch> batches) {
        this.batches = batches;
    }

    public OutgoingBatches() {

    }

    public Set<NodeChannel> getActiveChannels() {
        return activeChannels;
    }

    public void addActiveChannel(NodeChannel nodeChannel) {
        activeChannels.add(nodeChannel);
        activeChannelIds.add(nodeChannel.getId());
    }

    public void setActiveChannels(Set<NodeChannel> activeChannels) {
        this.activeChannels = activeChannels;
        activeChannelIds = new HashSet<String>();
        for (NodeChannel nodeChannel : activeChannels) {
            activeChannelIds.add(nodeChannel.getId());
        }
    }

    public List<OutgoingBatch> getBatches() {
        return batches;
    }

    public void setBatches(List<OutgoingBatch> batches) {
        this.batches = batches;
    }

    /**
     * Removes all batches associated with the provided channel from this
     * object.
     * 
     * @param channel
     *            - channel for which corresponding batches are removed
     * @return A list of the batches removed
     */
    public List<OutgoingBatch> filterBatchesForChannel(Channel channel) {
        List<OutgoingBatch> filtered = getBatchesForChannel(channel);
        batches.removeAll(filtered);
        return filtered;
    }

    public List<OutgoingBatch> filterBatchesForChannel(String channelId) {
        List<OutgoingBatch> filtered = getBatchesForChannel(channelId);
        batches.removeAll(filtered);
        return filtered;
    }

    public List<OutgoingBatch> filterBatchesForChannels(Set<String> channels) {
        List<OutgoingBatch> filtered = getBatchesForChannels(channels);
        batches.removeAll(filtered);
        return filtered;
    }

    public List<OutgoingBatch> getBatchesForChannel(Channel channel) {
        List<OutgoingBatch> batchList = new ArrayList<OutgoingBatch>();
        if (channel != null) {
            batchList = getBatchesForChannel(channel.getId());
        }
        return batchList;
    }

    public List<OutgoingBatch> getBatchesForChannel(String channelId) {
        List<OutgoingBatch> batchList = new ArrayList<OutgoingBatch>();
        if (channelId != null) {
            for (OutgoingBatch batch : batches) {
                if (channelId.equals(batch.getChannelId())) {
                    batchList.add(batch);
                }
            }
        }
        return batchList;
    }

    public List<OutgoingBatch> getBatchesForChannels(Set<String> channelIds) {
        List<OutgoingBatch> batchList = new ArrayList<OutgoingBatch>();
        if (channelIds != null) {
            for (OutgoingBatch batch : batches) {
                if (channelIds.contains(batch.getChannelId())) {
                    batchList.add(batch);
                }
            }
        }
        return batchList;
    }

    public List<OutgoingBatch> getBatchesForChannelWindows(Node targetNode, NodeChannel channel,
            List<NodeGroupChannelWindow> windows) {
        List<OutgoingBatch> keeping = new ArrayList<OutgoingBatch>();

        if (windows != null) {
            if (batches != null && batches.size() > 0) {
                if (channel.isEnabled() && inTimeWindow(windows, targetNode.getTimezoneOffset())) {
                    int max = channel.getMaxBatchToSend();
                    int count = 0;
                    for (OutgoingBatch outgoingBatch : batches) {
                        if (channel.getId().equals(outgoingBatch.getChannelId()) && count < max) {
                            keeping.add(outgoingBatch);
                            count++;
                        }
                    }
                }
            }
        }
        return keeping;
    }

    /**
     * If {@link NodeGroupChannelWindow}s are defined for this channel, then
     * check to see if the time (according to the offset passed in) is within on
     * of the configured windows.
     */
    public boolean inTimeWindow(List<NodeGroupChannelWindow> windows, String timezoneOffset) {
        if (windows != null && windows.size() > 0) {
            for (NodeGroupChannelWindow window : windows) {
                if (window.inTimeWindow(timezoneOffset)) {
                    return true;
                }
            }
            return false;
        } else {
            return true;
        }

    }

    /**
     * Removes all batches that are not associated with an 'activeChannel'.
     * 
     * @return List of batches that were filtered
     */

    public List<OutgoingBatch> filterBatchesForInactiveChannels() {
        List<OutgoingBatch> filtered = new ArrayList<OutgoingBatch>();

        for (OutgoingBatch batch : batches) {
            if (!activeChannelIds.contains(batch.getChannelId())) {
                filtered.add(batch);
            }
        }

        batches.removeAll(filtered);
        return filtered;
    }

}
