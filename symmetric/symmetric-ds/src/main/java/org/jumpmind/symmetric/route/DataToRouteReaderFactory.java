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

import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.springframework.jdbc.core.JdbcTemplate;

public class DataToRouteReaderFactory implements ISqlProvider {

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private IDataService dataService;

    private IParameterService parameterService;

    private Map<String, String> sql;

    public IDataToRouteReader getDataToRouteReader(RouterContext context) {
        String type = parameterService.getString(ParameterConstants.ROUTING_DATA_READER_TYPE);
        if (type == null || type.equals("ref")) {
            return new DataRefRouteReader(jdbcTemplate.getDataSource(),
                    jdbcTemplate.getQueryTimeout(), dbDialect.getRouterDataPeekAheadCount(), this,
                    dbDialect.getStreamingResultsFetchSize(), context, dataService, dbDialect.requiresAutoCommitFalseToSetFetchSize());
        } else if (type == null || type.equals("gap")) {
            return new DataGapRouteReader(jdbcTemplate.getDataSource(),
                    jdbcTemplate.getQueryTimeout(), dbDialect.getRouterDataPeekAheadCount(), this,
                    dbDialect.getStreamingResultsFetchSize(), context, dataService, dbDialect.requiresAutoCommitFalseToSetFetchSize());
        } else {
            throw unsupportedType(type);
        }
    }

    public IDataToRouteGapDetector getDataToRouteGapDetector() {
        String type = parameterService.getString(ParameterConstants.ROUTING_DATA_READER_TYPE);
        if (type == null || type.equals("ref")) {
            return new DataRefGapDetector(dataService, parameterService, jdbcTemplate, dbDialect,
                    this);
        } else if (type == null || type.equals("gap")) {
            return new DataGapDetector(dataService, parameterService, jdbcTemplate, dbDialect, this);
        } else {
            throw unsupportedType(type);
        }
    }

    private RuntimeException unsupportedType(String type) {
        return new UnsupportedOperationException("The data to route type of '" + type
                + "' is not supported");
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String getSql(String key) {
        return sql.get(key);
    }

    public void setSql(Map<String, String> sql) {
        this.sql = sql;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
