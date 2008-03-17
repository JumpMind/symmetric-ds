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

package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class AckServlet extends AbstractServlet {

    private static final long serialVersionUID = 1L;

    protected static final Log logger = LogFactory.getLog(AckServlet.class);

    @SuppressWarnings("unchecked")
    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        Map parameters = req.getParameterMap();
        List<BatchInfo> batches = new ArrayList<BatchInfo>();

        // TODO: acks should be saved in order received
        for (Object batch : parameters.keySet()) {
            String batchId = batch.toString();
            if (batchId.startsWith(WebConstants.ACK_BATCH_NAME)) {
                Object value = parameters.get(batch);
                String status = "";
                if (value instanceof String) {
                    status = (String) value;
                } else if (value instanceof String[]) {
                    String[] array = (String[]) value;
                    if (array.length > 0) {
                        status = array[0];
                    }
                }

                if (status.trim().equalsIgnoreCase(WebConstants.ACK_BATCH_OK)) {
                    batches.add(new BatchInfo(getBatchIdFrom(batchId)));
                } else {
                    try {
                        int lineNumber = Integer.parseInt(status.trim());
                        batches.add(new BatchInfo(getBatchIdFrom(batchId),
                                lineNumber));
                    } catch (NumberFormatException ex) {
                        batches.add(new BatchInfo(getBatchIdFrom(batchId),
                                BatchInfo.UNDEFINED_ERROR_LINE_NUMBER));
                    }
                }
            }
        }

        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());
        IAcknowledgeService service = (IAcknowledgeService) ctx
                .getBean(Constants.ACKNOWLEDGE_SERVICE);
        service.ack(batches);
    }

    private String getBatchIdFrom(String webParameter) {
        int index = WebConstants.ACK_BATCH_NAME.length();
        if (index >= 0) {
            return webParameter.substring(index);
        } else {
            throw new IllegalStateException("Invalid batch parameter "
                    + webParameter);
        }
    }

	@Override
	protected Log getLogger() {
		return logger;
	}

}
