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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.model.StatisticAlertThresholds;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.statistic.Statistic;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.jdbc.core.simple.ParameterizedRowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

public class StatisticService extends AbstractService implements IStatisticService {

    public void save(Collection<Statistic> stats, Date captureEndTime) {
        if (stats != null) {
            for (Statistic statistic : stats) {
                jdbcTemplate.update(getSql("insertStatisticSql"), new Object[] { statistic.getNodeId(),
                        AppUtils.getServerId(), statistic.getName().name(), statistic.getCaptureStartTimeMs(),
                        captureEndTime, statistic.getTotal(), statistic.getCount() });
            }
        }
    }

    public List<StatisticAlertThresholds> getAlertThresholds() {
        return getSimpleTemplate().query(getSql("getAlertThresholdsSql"),
                new ParameterizedRowMapper<StatisticAlertThresholds>() {
                    public StatisticAlertThresholds mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return new StatisticAlertThresholds(rs.getString("statistic_name"), rs
                                .getBigDecimal("threshold_total_max"), rs.getLong("threshold_count_max"), rs
                                .getBigDecimal("threshold_total_min"), rs.getLong("threshold_count_min"), rs
                                .getBigDecimal("threshold_avg_max"), rs.getBigDecimal("threshold_avg_min"));
                    }
                });
    }

    public void saveStatisticAlertThresholds(StatisticAlertThresholds threshold) {
        SimpleJdbcTemplate template = getSimpleTemplate();
        int updated = template.update(getSql("updateAlertThresholdsSql"), threshold.getThresholdTotalMax(), threshold
                .getThresholdCountMax(), threshold.getThresholdAvgMax(), threshold.getThresholdTotalMin(), threshold
                .getThresholdCountMin(), threshold.getThresholdAvgMin(), threshold.getStatisticName());
        if (updated == 0) {
            template.update(getSql("insertAlertThresholdsSql"), threshold.getStatisticName(), threshold
                    .getThresholdTotalMax(), threshold.getThresholdCountMax(), threshold.getThresholdAvgMax(),
                    threshold.getThresholdTotalMin(), threshold.getThresholdCountMin(), threshold
                            .getThresholdAvgMin());
        }
    }

    public boolean removeStatisticAlertThresholds(String statisticName) {
        return 1 == getSimpleTemplate().update(getSql("deleteAlertThresholdsSql"), statisticName);
    }
}
