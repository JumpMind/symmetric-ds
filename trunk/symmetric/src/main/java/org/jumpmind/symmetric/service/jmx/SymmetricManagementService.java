package org.jumpmind.symmetric.service.jmx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

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
}
