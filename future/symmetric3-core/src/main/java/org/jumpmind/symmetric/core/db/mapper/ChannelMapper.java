package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.Channel;

public class ChannelMapper implements ISqlRowMapper<Channel> {

    public Channel mapRow(Row row) {
        Channel channel = new Channel();
        channel.setChannelId(row.getString("CHANNEL_ID"));
        channel.setBatchAlgorithm(row.getString("BATCH_ALGORITHM"));
        channel.setContainsBigLob(row.getBoolean("CONTAINS_BIG_LOB"));
        channel.setEnabled(row.getBoolean("ENABLED"));
        channel.setExtractPeriodMillis(row.getLong("EXTRACT_PERIOD_MILLIS"));
        channel.setMaxBatchSize(row.getInt("MAX_BATCH_SIZE"));
        channel.setMaxBatchToSend(row.getInt("MAX_BATCH_TO_SEND"));
        channel.setMaxDataToRoute(row.getInt("MAX_DATA_TO_ROUTE"));
        channel.setProcessingOrder(row.getInt("PROCESSING_ORDER"));
        channel.setUseOldDataToRoute(row.getBoolean("USE_OLD_DATA_TO_ROUTE"));
        channel.setUsePkDataToRoute(row.getBoolean("USE_PK_DATA_TO_ROUTE"));
        channel.setUseRowDataToRoute(row.getBoolean("USE_ROW_DATA_TO_ROUTE"));
        return channel;
    }
}
