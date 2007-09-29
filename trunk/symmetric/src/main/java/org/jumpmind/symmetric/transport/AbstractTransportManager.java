package org.jumpmind.symmetric.transport;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;

import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.model.IncomingBatchHistory.Status;
import org.jumpmind.symmetric.web.WebConstants;

abstract public class AbstractTransportManager {

    protected static final String ENCODING = "UTF-8";
    
    protected String getAcknowledgementData(List<IncomingBatchHistory> list)
            throws IOException {
        StringBuilder builder = null;
        for (IncomingBatchHistory loadStatus : list) {
            Object value = null;
            if (loadStatus.getStatus() == Status.OK
                    || loadStatus.getStatus() == Status.SK) {
                value = WebConstants.ACK_BATCH_OK;
            } else {
                value = loadStatus.getFailedRowNumber();
            }
            builder = append(builder, WebConstants.ACK_BATCH_NAME
                    + loadStatus.getBatchId(), value);
        }
        return builder != null ? builder.toString() : "";
    }
    
    protected StringBuilder append(StringBuilder builder, String name,
            Object value) throws IOException {
        if (builder == null) {
            builder = new StringBuilder();
        } else {
            builder.append("&");
        }
        if (value == null) {
            value = "";
        }
        return builder.append(name).append("=").append(
                URLEncoder.encode(value.toString(), ENCODING));
    }
    
}
