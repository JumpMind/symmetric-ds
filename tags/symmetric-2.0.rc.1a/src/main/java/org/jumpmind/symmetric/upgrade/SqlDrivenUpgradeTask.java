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

package org.jumpmind.symmetric.upgrade;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.core.RowCallbackHandler;

public class SqlDrivenUpgradeTask extends AbstractSqlUpgradeTask {

    private static final ILog log = LogFactory.getLog(SqlDrivenUpgradeTask.class);

    protected String driverSql;

    protected String updateSql;

    public void upgrade(final String nodeId, final IParameterService parameterService, int[] fromVersion) {
        String sql = prepareSql(nodeId, parameterService, driverSql);
        log.warn("SqlForEachUpgrade", sql);
        log.warn("SqlDoUpgrade", updateSql);
        jdbcTemplate.query(sql, new RowCallbackHandler() {
            public void processRow(ResultSet rs) throws SQLException {
                int count = rs.getMetaData().getColumnCount();
                Object[] params = new Object[count];
                for (int i = 0; i < count; i++) {
                    params[i] = rs.getObject(i + 1);
                }
                jdbcTemplate.update(prepareSql(nodeId, parameterService, updateSql), params);
            }
        });
    }

    public void setDriverSql(String driverSql) {
        this.driverSql = driverSql;
    }

    public void setUpdateSql(String updateSql) {
        this.updateSql = updateSql;
    }

}
