package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.springframework.jdbc.core.JdbcTemplate;

abstract class AbstractService {

    protected IRuntimeConfig runtimeConfiguration;

    protected JdbcTemplate jdbcTemplate;

    public void setRuntimeConfiguration(IRuntimeConfig runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public void setJdbcTemplate(JdbcTemplate jdbc) {
        this.jdbcTemplate = jdbc;
    }

}
