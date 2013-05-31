package org.jumpmind.symmetric.statistic;

import java.io.Writer;

public class DataExtractorStatisticsWriter extends AbstractStatisticsWriter {

    public DataExtractorStatisticsWriter(IStatisticManager statisticManager, Writer out,
            int notifyAfterByteCount, int notifyAfterLineCount) {
        super(statisticManager, out, notifyAfterByteCount, notifyAfterLineCount);
    }

    @Override
    protected void processNumberOfBytesSoFar(long count) {
        if (channelId != null) {
            statisticManager.incrementDataBytesExtracted(channelId, count);
        }
    }

    @Override
    protected void processNumberOfLinesSoFar(long count) {
        if (channelId != null) {
            statisticManager.incrementDataExtracted(channelId, count);
        }
    }

}