/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
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
package org.jumpmind.symmetric.service.mock;

import java.util.List;

import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.springframework.jdbc.core.JdbcTemplate;

public class MockOutgoingBatchService implements IOutgoingBatchService {

    public void markAllAsSentForNode(String nodeId) {
    }

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId) {
        return false;
    }

    public void buildOutgoingBatches(String nodeId, List<NodeChannel> channels) {

    }

    public void buildOutgoingBatches(String nodeId, NodeChannel channel) {

    }

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId) {
        return null;
    }

    public OutgoingBatches getOutgoingBatchErrors(int maxRows) {
        return null;
    }

    public OutgoingBatches getOutgoingBatches(String nodeId) {
        return null;
    }

    public void insertOutgoingBatch(JdbcTemplate template, OutgoingBatch outgoingBatch) {

    }

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch) {

    }

    public boolean isInitialLoadComplete(String nodeId) {
        return false;
    }

    public void markOutgoingBatchSent(OutgoingBatch batch) {

    }

    public void setBatchStatus(long batchId, Status status) {

    }

    public OutgoingBatch findOutgoingBatch(long batchId) {
        return null;
    }

    public void updateOutgoingBatch(JdbcTemplate jdbcTemplate, OutgoingBatch batch) {

    }

    public void updateOutgoingBatch(OutgoingBatch batch) {

    }
}
