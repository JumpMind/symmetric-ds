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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;

import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticName;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for outgoing synchronization")
public class OutgoingManagementService {

    private IStatisticManager statisticManager;
    
    private IDataExtractorService dataExtractorService;

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    @ManagedAttribute(description = "Get the average number of events in each batch since the last statistic flush")
    public BigDecimal getPeriodicAverageEventsPerBatch() {
        return this.statisticManager.getStatistic(StatisticName.OUTGOING_EVENTS_PER_BATCH).getAverageValue();
    }

    @ManagedAttribute(description = "Get the average number of events in each batch for the lifetime of the server")
    public BigDecimal getServerLifetimeAverageEventsPerBatch() {
        return this.statisticManager.getStatistic(StatisticName.OUTGOING_EVENTS_PER_BATCH).getLifetimeAverageValue();
    }

    @ManagedAttribute(description = "Get the average number of milliseconds it takes to batch an event since the last statistic flush")
    public BigDecimal getPeriodicAverageMsPerEventBatched() {
        return this.statisticManager.getStatistic(StatisticName.OUTGOING_MS_PER_EVENT_BATCHED).getAverageValue();
    }

    @ManagedAttribute(description = "Get the average number of milliseconds it takes to batch an event for the lifetime of the server")
    public BigDecimal getServerLifetimeAverageMsPerEventBatched() {
        return this.statisticManager.getStatistic(StatisticName.OUTGOING_MS_PER_EVENT_BATCHED)
                .getLifetimeAverageValue();
    }
    
    @ManagedOperation(description = "Show a batch in SymmetricDS Data Format.")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "batchId", description = "The batch ID to display") })
    public String showBatch(String batchId) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOutgoingTransport transport = new InternalOutgoingTransport(out);
        dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
        out.close();
        return out.toString();
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }
}
