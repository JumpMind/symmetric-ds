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

import java.util.Map;

import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedOperationParameter;
import org.springframework.jmx.export.annotation.ManagedOperationParameters;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(description = "The management interface for node parameters")
public class ParameterManagementService {

    private IParameterService parameterService;

    @ManagedOperation(description = "Reload supported parameters from file or database")
    public void rereadParameters() {
        this.parameterService.rereadParameters();
    }

    @ManagedOperation(description = "Update a parameter for this node only")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "key", description = "The name of the parameter"),
            @ManagedOperationParameter(name = "value", description = "The value for the parameter") })
    public void updateParameter(String key, String value) {
        this.parameterService.saveParameter(key, value);
    }

    @ManagedOperation(description = "Update a parameter for all nodes")
    @ManagedOperationParameters( { @ManagedOperationParameter(name = "key", description = "The name of the parameter"),
            @ManagedOperationParameter(name = "value", description = "The value for the parameter") })
    public void updateParameterForAll(String key, String value) {
        this.parameterService.saveParameter(IParameterService.ALL, IParameterService.ALL, key, value);
    }

    @ManagedOperation(description = "Update a parameter for all nodes in a group")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "nodeGroup", description = "The name of the node group"),
            @ManagedOperationParameter(name = "key", description = "The name of the parameter"),
            @ManagedOperationParameter(name = "value", description = "The value for the parameter") })
    public void updateParameterForNodeGroup(String nodeGroup, String key, String value) {
        this.parameterService.saveParameter(IParameterService.ALL, nodeGroup, key, value);
    }

    @ManagedOperation(description = "Update a parameter for a specific node")
    @ManagedOperationParameters( {
            @ManagedOperationParameter(name = "externalId", description = "The name of the external id of node"),
            @ManagedOperationParameter(name = "nodeGroup", description = "The name of the node group"),
            @ManagedOperationParameter(name = "key", description = "The name of the parameter"),
            @ManagedOperationParameter(name = "value", description = "The value for the parameter") })
    public void updateParameterForNode(String externalId, String nodeGroup, String key, String value) {
        this.parameterService.saveParameter(externalId, nodeGroup, key, value);
    }

    @ManagedAttribute(description = "The parameters configured for this SymmetricDS instance")
    public String getParametersList() {
        StringBuilder buffer = new StringBuilder();
        Map<String, String> params = parameterService.getAllParameters();
        buffer.append("<pre>");
        for (String key : params.keySet()) {
            buffer.append(key).append("=").append(params.get(key)).append("\n");
        }
        buffer.append("</pre>");
        return buffer.toString();
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
}
