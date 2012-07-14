package org.jumpmind.symmetric.core.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.db.Query;
import org.jumpmind.symmetric.core.db.mapper.ChannelMapper;
import org.jumpmind.symmetric.core.db.mapper.NodeChannelControlMapper;
import org.jumpmind.symmetric.core.model.Channel;
import org.jumpmind.symmetric.core.model.Node;
import org.jumpmind.symmetric.core.model.NodeChannel;
import org.jumpmind.symmetric.core.model.NodeChannelControl;
import org.jumpmind.symmetric.core.model.NodeGroup;
import org.jumpmind.symmetric.core.model.NodeSecurity;
import org.jumpmind.symmetric.core.model.Parameters;

public class ConfigurationService extends AbstractParameterizedService {

    protected Map<String, List<NodeChannel>> nodeChannelCache;

    protected static final ChannelMapper CHANNEL_MAPPER = new ChannelMapper();

    protected static final NodeChannelControlMapper NODE_CHANNEL_CONTROL_MAPPER = new NodeChannelControlMapper();

    protected static final Channel[] DEFAULT_CHANNELS = new Channel[] {
            new Channel("config", 0, 100, 100, true, 0), new Channel("reload", 1, 1, 100, true, 0), };

    protected long nodeChannelCacheTime;

    protected List<Channel> defaultChannels;

    protected NodeService nodeService;

    public ConfigurationService(IEnvironment environment, ParameterService parameterSerivce,
            NodeService nodeService) {
        super(environment, parameterSerivce);
        this.nodeService = nodeService;
    }

    public void autoConfigTables() {
        this.dbDialect.alter(true, this.tables.getTables());
    }

    public void autoConfigFunctions() {
        dbDialect.getDataCaptureBuilder().configureRequiredFunctions();
    }

    public void autoConfigChannels() {
        if (defaultChannels != null) {
            reloadChannels();
            List<NodeChannel> channels = findNodeChannels(false);
            for (Channel defaultChannel : defaultChannels) {
                if (!defaultChannel.isInList(channels)) {
                    log.info("Auto-configuring %s channel.", defaultChannel.getChannelId());
                    saveChannel(defaultChannel, true);
                } else {
                    log.info("No need to create channel %s.  It already exists.", defaultChannel.getChannelId());
                }
            }
            reloadChannels();
        }
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.CHANNEL), getParams(channel));
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public List<NodeChannel> findNodeChannels(boolean refreshExtractMillis) {
        return findNodeChannels(nodeService.findIdentityNodeId(), refreshExtractMillis);
    }

    public List<NodeChannel> findNodeChannels(final String nodeId, boolean refreshExtractMillis) {
        boolean loaded = false;

        long channelCacheTimeoutInMs = getParameters().getLong(
                Parameters.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        List<NodeChannel> nodeChannels = nodeChannelCache != null ? nodeChannelCache.get(nodeId)
                : null;
        if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                || nodeChannels == null) {
            synchronized (this) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                        || nodeChannelCache == null || nodeChannelCache.get(nodeId) == null
                        || nodeChannels == null) {
                    if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                            || nodeChannelCache == null) {
                        nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                        nodeChannelCacheTime = System.currentTimeMillis();
                    }

                    nodeChannels = selectNodeChannels(nodeId);
                    nodeChannelCache.put(nodeId, nodeChannels);
                    loaded = true;
                }
            }
        }

        if (!loaded && refreshExtractMillis) {
            // need to read last extracted time from database regardless of
            // whether we used the cache or not.
            // locate the nodes in the cache, and update it.
            List<NodeChannelControl> controls = selectNodeChannelControlsFor(nodeId);

            for (NodeChannel nodeChannel : nodeChannels) {
                for (NodeChannelControl nodeChannelControl : controls) {
                    nodeChannel.getNodeChannelControl().setLastExtractTime(
                            nodeChannelControl.getLastExtractTime());
                }
            }
        }

        return nodeChannels;
    }

    public List<NodeChannel> selectNodeChannels(String nodeId) {
        List<Channel> channels = selectChannels();
        List<NodeChannel> nodeChannels = new ArrayList<NodeChannel>(channels.size());
        List<NodeChannelControl> nodeChannelControls = selectNodeChannelControlsFor(nodeId);
        for (Channel channel : channels) {
            NodeChannelControl foundNodeChannelCtl = null;
            for (NodeChannelControl nodeChannelControl : nodeChannelControls) {
                if (nodeChannelControl.getChannelId().equals(channel.getChannelId())) {
                    foundNodeChannelCtl = nodeChannelControl;
                    break;
                }
            }
            nodeChannels.add(new NodeChannel(channel, foundNodeChannelCtl));
        }
        return nodeChannels;
    }

    public List<Channel> selectChannels() {
        return dbDialect.getSqlTemplate().query(
                new Query(dbDialect.getDbDialectInfo().getIdentifierQuoteString(), 0,
                        getTable(SymmetricTables.CHANNEL)), CHANNEL_MAPPER);
    }

    public void saveNodeChannelControl(NodeChannelControl nodeChannelCtl, boolean reloadChannels) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.NODE_CHANNEL_CTL),
                getParams(nodeChannelCtl));
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public List<NodeChannelControl> selectNodeChannelControlsFor(String nodeId) {
        return dbDialect.getSqlTemplate().query(
                new Query(dbDialect.getDbDialectInfo().getIdentifierQuoteString(), 1,
                        getTable(SymmetricTables.NODE_CHANNEL_CTL)).where("NODE_ID", "=", nodeId),
                NODE_CHANNEL_CONTROL_MAPPER);
    }

    public void autoConfigRegistrationServer() {
        Node node = nodeService.findIdentity();

        // TODO - Not needed for android client
        // if (node == null) {
        // buildTablesFromDdlUtilXmlIfProvided();
        // loadFromScriptIfProvided();
        // }
        Parameters parameters = getParameters();
        if (node == null && StringUtils.isBlank(parameters.getRegistrationUrl())
                && parameters.is(Parameters.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
            log.info("Inserting rows for node, security, identity and group for registration server");
            String nodeGroupId = parameters.getNodeGroupId();
            String nodeId = parameters.getExternalId();
            try {
                node = new Node(nodeId, nodeGroupId);
                node.setCreatedAtNodeId(nodeId);
                node.setSyncUrl(parameters.getSyncUrl());
                node.setSyncEnabled(true);
                node.setHeartbeatTime(new Date());
                nodeService.saveNode(node);
            } catch (DataIntegrityViolationException ex) {
                log.warn("Not inserting node row for %s because it already exists", nodeId);
            }
            nodeService.insertNodeIdentity(nodeId);

            node = nodeService.findIdentity();

            nodeService.saveNodeGroup(new NodeGroup(nodeGroupId));
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId, true);
            nodeSecurity.setInitialLoadTime(new Date());
            nodeSecurity.setRegistrationTime(new Date());
            nodeSecurity.setInitialLoadEnabled(false);
            nodeSecurity.setRegistrationEnabled(false);
            nodeService.saveNodeSecurity(nodeSecurity);
        }
    }

    public void reloadChannels() {
        synchronized (this) {
            nodeChannelCache = null;
        }
    }

    public Map<String, Object> getParams(Channel channel) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("CHANNEL_ID", channel.getChannelId());
        params.put("BATCH_ALGORITHM", channel.getBatchAlgorithm());
        params.put("CONTAINS_BIG_LOB", channel.isContainsBigLob());
        params.put("ENABLED", channel.isEnabled());
        params.put("EXTRACT_PERIOD_MILLIS", channel.getExtractPeriodMillis());
        params.put("MAX_BATCH_SIZE", channel.getMaxBatchSize());
        params.put("MAX_BATCH_TO_SEND", channel.getMaxBatchToSend());
        params.put("MAX_DATA_TO_ROUTE", channel.getMaxDataToRoute());
        params.put("PROCESSING_ORDER", channel.getProcessingOrder());
        params.put("USE_OLD_DATA_TO_ROUTE", channel.isUseOldDataToRoute());
        params.put("USE_PK_DATA_TO_ROUTE", channel.isUsePkDataToRoute());
        params.put("USE_ROW_DATA_TO_ROUTE", channel.isUseRowDataToRoute());
        return params;
    }

    public Map<String, Object> getParams(NodeChannelControl control) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("CHANNEL_ID", control.getChannelId());
        params.put("IGNORE_ENABLED", control.isIgnoreEnabled());
        params.put("LAST_EXTRACT_TIME", control.getLastExtractTime());
        params.put("NODE_ID", control.getNodeId());
        params.put("SUSPEND_ENABLED", control.isSuspendEnabled());
        return params;
    }

}
