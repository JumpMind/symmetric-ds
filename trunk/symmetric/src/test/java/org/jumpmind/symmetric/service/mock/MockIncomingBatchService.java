/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
 *               
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

import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IIncomingBatchService;

public class MockIncomingBatchService implements IIncomingBatchService {

    public boolean acquireIncomingBatch(IncomingBatch status) {

        return false;
    }

    public IncomingBatch findIncomingBatch(long batchId, String nodeId) {

        return null;
    }

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows) {

        return null;
    }

    public List<IncomingBatchHistory> findIncomingBatchHistory(long batchId, String nodeId) {

        return null;
    }

    public void insertIncomingBatch(IncomingBatch status) {

    }

    public void insertIncomingBatchHistory(IncomingBatchHistory history) {

    }

    public int updateIncomingBatch(IncomingBatch status) {

        return 0;
    }

}
