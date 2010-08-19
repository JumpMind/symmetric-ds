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
package org.jumpmind.symmetric.service.impl;

import org.hsqldb.Types;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.ChannelStats;

public class StatisticService extends AbstractService implements IStatisticService {

    public void save(ChannelStats stats) {
        jdbcTemplate.update(getSql("insertChannelStatsSql"), new Object[] { stats.getNodeId(),
                stats.getHostName(), stats.getChannelId(), stats.getStartTime(),
                stats.getEndTime(), stats.getDataRouted(), stats.getDataUnRouted(),
                stats.getDataEventInserted(), stats.getDataExtracted(),
                stats.getDataBytesExtracted(), stats.getDataExtractedErrors(),
                stats.getDataTransmitted(), stats.getDataBytesTransmitted(),
                stats.getDataTransmittedErrors(), stats.getDataLoaded(),
                stats.getDataBytesLoaded(), stats.getDataLoadedErrors() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT });
    }
}
