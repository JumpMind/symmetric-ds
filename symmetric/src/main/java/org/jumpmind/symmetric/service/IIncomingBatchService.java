/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.springframework.transaction.annotation.Transactional;

public interface IIncomingBatchService {

    public IncomingBatch findIncomingBatch(String batchId, String clientId);

    public List<IncomingBatchHistory> findIncomingBatchHistory(String batchId, String clientId);

    @Transactional
    public boolean acquireIncomingBatch(IncomingBatch status);
    
    @Transactional
    public void insertIncomingBatch(IncomingBatch status);
    
    @Transactional
    public void insertIncomingBatchHistory(IncomingBatchHistory history);
    
    @Transactional
    public int updateIncomingBatch(IncomingBatch status);
}
