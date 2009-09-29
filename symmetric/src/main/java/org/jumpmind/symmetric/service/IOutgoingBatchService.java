/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>                      
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

import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.springframework.jdbc.core.JdbcTemplate;

public interface IOutgoingBatchService {

    public void markAllAsSentForNode(String nodeId);

    public OutgoingBatch findOutgoingBatch(long batchId); 
    
    public OutgoingBatches getOutgoingBatches(String nodeId);

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId);

    public OutgoingBatches getOutgoingBatchErrors(int maxRows);

    public boolean isInitialLoadComplete(String nodeId);

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId);

    public void updateOutgoingBatch(OutgoingBatch batch);

    public void updateOutgoingBatch(JdbcTemplate jdbcTemplate, OutgoingBatch batch);

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch);

    public void insertOutgoingBatch(JdbcTemplate jdbcTemplate, OutgoingBatch outgoingBatch);

}
