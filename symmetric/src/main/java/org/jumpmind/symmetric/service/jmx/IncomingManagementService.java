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
package org.jumpmind.symmetric.service.jmx;

import java.math.BigDecimal;

import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for incoming synchronization")
public class IncomingManagementService {

    IStatisticManager statisticManager;

    //    INCOMING_TRANSPORT_ERROR_COUNT,
    //    INCOMING_TRANSPORT_CONNECT_ERROR_COUNT,
    //    INCOMING_TRANSPORT_REJECTED_COUNT,
    //    INCOMING_DATABASE_ERROR_COUNT,
    //    INCOMING_OTHER_ERROR_COUNT,
    //    INCOMING_MS_PER_ROW,
    //    INCOMING_BATCH_COUNT,
    //    INCOMING_SKIP_BATCH_COUNT,

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    @ManagedAttribute(description = "Get the number of milliseconds the system is currently taking to commit a data row coming in from another node since the last time statistics were flushed")
    public BigDecimal getDatabaseMsPerRowSinceLastFlush() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_MS_PER_ROW).getAverageValue();
    }

    @ManagedAttribute(description = "Get the number of milliseconds the system is currently taking to commit a data row coming in from another node for the lifetime of the server")
    public BigDecimal getDatabaseMsPerRowForServerLifetime() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_MS_PER_ROW)
                .getLifetimeAverageValue();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during transport since the last flush")
    public BigDecimal getTransportErrorCountSinceLastFlush() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_ERROR_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during transport for the lifetime of the server")
    public BigDecimal getTransportErrorCountForServerLifetime() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_ERROR_COUNT)
                .getLifetimeTotal();
    }

}
