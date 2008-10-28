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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

abstract class AbstractService {

    protected IParameterService parameterService;

    protected JdbcTemplate jdbcTemplate;

    private Map<String, String> sql;

    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbcTemplate = jdbc;
    }
    
    protected SimpleJdbcTemplate getSimpleTemplate() {
        return new SimpleJdbcTemplate(jdbcTemplate);
    }
    
    @SuppressWarnings("unchecked")
    protected SQLException unwrapSqlException(Throwable e) {
        List<Throwable> exs = ExceptionUtils.getThrowableList(e);
        for (Throwable throwable : exs) {
            if (throwable instanceof SQLException) {
                return (SQLException) throwable;
            }
        }
        return null;
    }    

    public void setSql(Map<String, String> sql) {
        this.sql = sql;
    }

    public String getSql(String key) {
        return sql.get(key);
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
