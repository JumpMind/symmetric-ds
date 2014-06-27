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

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    @ManagedAttribute(description = "Get the number of milliseconds the system is currently taking to commit a data row coming in from another node since the last time statistics were flushed")
    public BigDecimal getPeriodicDatabaseMsPerRow() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_MS_PER_ROW).getAverageValue();
    }

    @ManagedAttribute(description = "Get the number of milliseconds the system is currently taking to commit a data row coming in from another node for the lifetime of the server")
    public BigDecimal getServerLifetimeDatabaseMsPerRow() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_MS_PER_ROW).getLifetimeAverageValue();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during transport since the last flush")
    public BigDecimal getPeriodicTransportErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_ERROR_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during transport for the lifetime of the server")
    public BigDecimal getServerLifetimeTransportErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_ERROR_COUNT).getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred while attempting to connect to transport since the last flush")
    public BigDecimal getPeriodicTransportConnectErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred while attempting to connect to transport for the lifetime of the server")
    public BigDecimal getServerLifetimeTransportConnectErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_CONNECT_ERROR_COUNT)
                .getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of rejections that occurred while attempting to connect to transport since the last flush")
    public BigDecimal getPeriodicTransportRejectedCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_REJECTED_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of rejections that occurred while attempting to connect to transport for the lifetime of the server")
    public BigDecimal getServerLifetimeTransportRejectedCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_TRANSPORT_REJECTED_COUNT).getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during database activity since the last flush")
    public BigDecimal getPeriodicDatabaseErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_DATABASE_ERROR_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of errors that occurred during database activity for the lifetime of the server")
    public BigDecimal getServerLifetimeDatabaseErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_DATABASE_ERROR_COUNT).getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of unanticipated errors that occurred since the last flush")
    public BigDecimal getPeriodicOtherErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_OTHER_ERROR_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of unanticipated errors that occurred for the lifetime of the server")
    public BigDecimal getServerLifetimeOtherErrorCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_OTHER_ERROR_COUNT).getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of batches that have been processed since the last flush")
    public BigDecimal getPeriodicBatchCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_BATCH_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of batches that have been processed for the lifetime of the server")
    public BigDecimal getServerLifetimeBatchCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_BATCH_COUNT).getLifetimeTotal();
    }

    @ManagedAttribute(description = "Get the number of batches that have been skipped since the last flush")
    public BigDecimal getPeriodicSkipBatchCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_SKIP_BATCH_COUNT).getTotal();
    }

    @ManagedAttribute(description = "Get the number of batches that have been skipped for the lifetime of the server")
    public BigDecimal getServerLifetimeSkipBatchCount() {
        return this.statisticManager.getStatistic(StatisticName.INCOMING_SKIP_BATCH_COUNT).getLifetimeTotal();
    }

}
