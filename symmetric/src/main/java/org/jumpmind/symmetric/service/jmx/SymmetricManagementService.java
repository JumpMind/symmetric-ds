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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for symmetric")
public class SymmetricManagementService {

    private IRuntimeConfig runtimeConfiguration;

    private IBootstrapService bootstrapService;

    private IPurgeService purgeService;

    private Properties properties;

    private DataSource dataSource;

    @ManagedOperation(description = "Run the purge process")
    public void purge() {
        purgeService.purge();
    }

    @ManagedOperation(description = "Synchronize the triggers")
    public void syncTriggers() {
        bootstrapService.syncTriggers();
    }

    @ManagedAttribute(description = "The properties configured for this symmetric instance")
    public String getPropertiesList() {
        StringWriter writer = new StringWriter();
        properties.list(new PrintWriter(writer, true));
        return writer.getBuffer().toString();
    }

    @ManagedAttribute(description = "The group this node belongs to")
    public String getNodeGroupId() {
        return runtimeConfiguration.getNodeGroupId();
    }

    @ManagedAttribute(description = "An external name give to this symmetric node")
    public String getExternalId() {
        return runtimeConfiguration.getExternalId();
    }

    @ManagedAttribute(description = "Whether the basic data source is being used as the default datasource.")
    public boolean isBasicDataSource() {
        return dataSource instanceof BasicDataSource;
    }

    @ManagedAttribute(description = "If a BasicDataSource, then show the number of active connections.")
    public int getNumberOfActiveConnections() {
        if (isBasicDataSource()) {
            return ((BasicDataSource) dataSource).getNumActive();
        } else {
            return -1;
        }
    }

    public void setRuntimeConfiguration(IRuntimeConfig runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    public void setPurgeService(IPurgeService purgeService) {
        this.purgeService = purgeService;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
