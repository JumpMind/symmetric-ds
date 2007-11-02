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

import java.util.List;

import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;

public interface IOutgoingBatchService {
    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch);

    public void buildOutgoingBatches(String nodeId);

    public List<OutgoingBatch> getOutgoingBatches(String nodeId);

    public List<OutgoingBatch> getOutgoingBatchRange(String startBatchId, String endBatchId);

    public void markOutgoingBatchSent(OutgoingBatch batch);

    public void setBatchStatus(String batchId, Status status);

    public boolean isInitialLoadComplete(String nodeId);
}
