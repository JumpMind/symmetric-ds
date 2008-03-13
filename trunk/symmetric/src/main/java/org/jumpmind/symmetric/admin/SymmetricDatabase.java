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
package org.jumpmind.symmetric.admin;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.service.IConfigurationService;

public class SymmetricDatabase implements Serializable {

    private static final long serialVersionUID = 1306181988155432278L;

    private String name;

    private String jdbcUrl;

    private String username;

    private String password;

    private String driverName;

    private transient SymmetricEngine engine;

    public SymmetricDatabase() {
    }

    public SymmetricDatabase(String name) {
        super();
        this.name = name;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void connect() {
        synchronized (SymmetricDatabase.class) {
            System.setProperty("db.url", jdbcUrl);
            System.setProperty("db.driver", driverName);
            System.setProperty("db.user", username == null ? "" : username);
            System.setProperty("db.password", password == null ? "" : password);
            System.setProperty("db.pool.initial.size", "1");
            engine = new SymmetricEngine();
        }
    }
    
    public List<NodeChannel> getChannels() {
        if (engine != null) {
            IConfigurationService configService = (IConfigurationService)engine.getApplicationContext().getBean(Constants.CONFIG_SERVICE);
            return configService.getChannelsFor(true);
        } else {
            return new ArrayList<NodeChannel>(0);
        }
    }
    
    public void disconnect() {
        // TODO
    }

    public boolean isConnected() {
        return engine != null;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

}
