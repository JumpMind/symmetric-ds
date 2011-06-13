package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.core.IEnvironment;

public class ConfigurationService extends AbstractParameterizedService {

    public ConfigurationService(IEnvironment environment, ParameterService parameterSerivce) {
        super(environment, parameterSerivce);
    }
    
    public void autoConfigTables() {
        this.dbDialect.alter(true, this.tables.getTables());        
    }
    
    public void autoConfigFunctions() {

    }
    
    public void autoConfigChannels() {
//        if (defaultChannels != null) {
//            reloadChannels();
//            List<NodeChannel> channels = getNodeChannels(false);
//            for (Channel defaultChannel : defaultChannels) {
//                if (!defaultChannel.isInList(channels)) {
//                    log.info("ChannelAutoConfiguring", defaultChannel.getChannelId());
//                    saveChannel(defaultChannel, true);
//                } else {
//                    log.info("ChannelExists", defaultChannel.getChannelId());
//                }
//            }
//            reloadChannels();
//        }
    }

    public void autoConfigRegistrationServer() {
//        Node node = nodeService.findIdentity();
//
//        if (node == null) {
//            buildTablesFromDdlUtilXmlIfProvided();
//            loadFromScriptIfProvided();
//        }
//
//        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl())
//                && parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
//            log.info("AutoConfigRegistrationService");
//            String nodeGroupId = parameterService.getNodeGroupId();
//            String nodeId = parameterService.getExternalId();
//            try {
//                nodeService.insertNode(nodeId, nodeGroupId, nodeId, nodeId);
//            } catch (DataIntegrityViolationException ex) {
//                log.warn("AutoConfigNodeIdAlreadyExists", nodeId);
//            }
//            nodeService.insertNodeIdentity(nodeId);
//            node = nodeService.findIdentity();
//            node.setSyncUrl(parameterService.getSyncUrl());
//            node.setSyncEnabled(true);
//            node.setHeartbeatTime(new Date());
//            nodeService.updateNode(node);
//            nodeService.insertNodeGroup(node.getNodeGroupId(), null);
//            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId, true);
//            nodeSecurity.setInitialLoadTime(new Date());
//            nodeSecurity.setRegistrationTime(new Date());
//            nodeSecurity.setInitialLoadEnabled(false);
//            nodeSecurity.setRegistrationEnabled(false);
//            nodeService.updateNodeSecurity(nodeSecurity);
//        }
    }

}
