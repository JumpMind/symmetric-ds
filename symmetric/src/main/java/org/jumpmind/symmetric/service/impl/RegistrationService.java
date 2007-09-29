package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;

public class RegistrationService extends AbstractService implements
        IRegistrationService {

    protected static final Log logger = LogFactory
            .getLog(RegistrationService.class);

    private INodeService nodeService;

    private IDataExtractorService dataExtractorService;

    private IConfigurationService configurationService;

    private String findClientToRegisterSql;

    private String registerClientSql;

    private String registerClientSecuritySql;

    private String reopenRegistrationSql;

    private String openRegistrationClientSql;

    private String openRegistrationClientSecuritySql;

    /**
     * Register a client for the given domain name and domain ID if the
     * registration is open.
     */
    public boolean registerNode(Node node, OutputStream out) throws IOException {
        String clientId = (String) jdbcTemplate.queryForObject(
                findClientToRegisterSql, new Object[] { node.getGroupId(),
                        node.getExternalId() }, String.class);
        if (clientId == null) {
            return false;
        }
        node.setNodeId(clientId);
        jdbcTemplate.update(registerClientSecuritySql, new Object[] { node
                .getNodeId() });
        jdbcTemplate.update(registerClientSql, new Object[] {
                node.getSyncURL().toString(), node.getSchemaVersion(),
                node.getDatabaseType(), node.getDatabaseVersion(),
                node.getSymmetricVersion(), node.getNodeId() });
        return writeConfiguration(node, out);
    }

    /**
     * Synchronize client configuration.
     */
    protected boolean writeConfiguration(Node node, OutputStream out)
            throws IOException {
        boolean written = false;
        IOutgoingTransport transport = new InternalOutgoingTransport(out);
        List<String> tableNames = configurationService
                .getConfigChannelTableNames();
        if (tableNames != null && tableNames.size() > 0) {
            for (String tableName : tableNames) {
                Trigger trigger = configurationService
                        .getTriggerForTarget(tableName, runtimeConfiguration
                                .getNodeGroupId(), node.getGroupId(),
                                Constants.CHANNEL_CONFIG);
                if (trigger != null) {
                    dataExtractorService.extractInitialLoadFor(node,
                            trigger, transport);
                }
            }
            dataExtractorService.extractClientIdentityFor(node, transport);
            written = true;
        } else {
            logger
                    .error("There were no configuration tables to return to the client.  There is a good chance that the system is configured incorrectly.");
        }
        transport.close();
        return written;
    }

    /**
     * Re-open registration for a single client that already exists in the
     * database. A new password is generated and the registration_enabled flag
     * is turned on. The next client to try registering for this domain name and
     * domain ID will be given this information.
     */
    public void reOpenRegistration(String clientId) {
        String password = generatePassword();
        jdbcTemplate.update(reopenRegistrationSql, new Object[] { password,
                clientId });
    }

    /**
     * Open registration for a single new client given a domain (f.e., "STORE")
     * and domain ID (f.e., "00001"). The unique client ID and password are
     * generated and stored in the client and client_security tables with the
     * registration_enabled flag turned on. The next client to try registering
     * for this domain name and domain ID will be given this information.
     */
    public void openRegistration(String nodeGroup, String externalid) {
        String clientId = generateClientId(nodeGroup, externalid);
        String password = generatePassword();
        jdbcTemplate.update(openRegistrationClientSql, new Object[] { clientId,
                nodeGroup, externalid });
        jdbcTemplate.update(openRegistrationClientSecuritySql, new Object[] {
                clientId, password });
    }

    /**
     * Generate a secure random password for a client.
     */
    // TODO: clientGenerator.generatePassword();
    protected String generatePassword() {
        return new RandomDataImpl().nextSecureHexString(30);
    }

    /**
     * Generate the next client ID that is available. Try to use the domain ID
     * as the client ID.
     */
    // TODO: clientGenerator.generateClientId();
    protected String generateClientId(String nodeGroupId, String externalId) {
        String clientId = externalId;
        int maxTries = 100;
        for (int sequence = 0; sequence < maxTries; sequence++) {
            if (nodeService.findNode(clientId) == null) {
                return clientId;
            }
            clientId = externalId + "-" + sequence;
        }
        throw new RuntimeException("Could not find clientId for domainId of "
                + externalId + " after " + maxTries + " tries.");
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setOpenRegistrationClientSecuritySql(
            String openRegistrationClientSecuritySql) {
        this.openRegistrationClientSecuritySql = openRegistrationClientSecuritySql;
    }

    public void setOpenRegistrationClientSql(String openRegistrationClientSql) {
        this.openRegistrationClientSql = openRegistrationClientSql;
    }

    public void setRegisterClientSecuritySql(String registerClientSecuritySql) {
        this.registerClientSecuritySql = registerClientSecuritySql;
    }

    public void setRegisterClientSql(String registerClientSql) {
        this.registerClientSql = registerClientSql;
    }

    public void setReopenRegistrationSql(String reopenRegistrationSql) {
        this.reopenRegistrationSql = reopenRegistrationSql;
    }

    public void setFindClientToRegisterSql(String findClientToRegisterSql) {
        this.findClientToRegisterSql = findClientToRegisterSql;
    }

    public void setDataExtractorService(
            IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setConfigurationService(
            IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}
