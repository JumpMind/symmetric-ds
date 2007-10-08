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
