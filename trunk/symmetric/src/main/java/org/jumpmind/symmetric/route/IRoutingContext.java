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

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.core.JdbcTemplate;

public interface IRoutingContext {

    /**
     * Get the same template that is being used for inserts into data_event for routing.
     */
    public JdbcTemplate getJdbcTemplate();

    public NodeChannel getChannel();

    public Map<String, OutgoingBatch> getBatchesByNodes();

    public Map<String, OutgoingBatchHistory> getBatchHistoryByNodes();

    public Map<Trigger, Set<Node>> getAvailableNodes();

    public void commit() throws SQLException;

    public void rollback();

    public void cleanup();

    public void setRouted(boolean b);

    public boolean isNeedsCommitted();

    public boolean isRouted();

    public void setNeedsCommitted(boolean b);

    public void resetForNextData();

    public void setEncountedTransactionBoundary(boolean encountedTransactionBoundary);

    public boolean isEncountedTransactionBoundary();
    
}
