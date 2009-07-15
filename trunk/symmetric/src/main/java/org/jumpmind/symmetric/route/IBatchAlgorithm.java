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
package org.jumpmind.symmetric.route;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;

/**
 * A possible extension point that could be configured by channels to allow further control over batching algorithms.  I am thinking that
 * we provide two implementations that can be configured at the channel level:  our default batching based on the number of events and another
 * implementation that would batch only on transaction boundaries.
 * @since 2.0
 */
public interface IBatchAlgorithm extends IExtensionPoint {  
    public boolean completeBatch(NodeChannel channel, OutgoingBatchHistory history, OutgoingBatch batch, Data data, boolean databaseTransactionBoundary);
}
