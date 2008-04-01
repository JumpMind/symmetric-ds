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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class AckServlet extends HttpServlet {
    private static final BatchIdComparator BATCH_ID_COMPARATOR = new BatchIdComparator();

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp)
            throws ServletException, IOException {
        final Map parameters = req.getParameterMap();
        final List<BatchInfo> batches = new ArrayList<BatchInfo>();

        for (final Object batch : parameters.keySet()) {
            final String batchId = batch.toString();
            if (batchId.startsWith(WebConstants.ACK_BATCH_NAME)) {
                final Object value = parameters.get(batch);
                String status = "";
                if (value instanceof String) {
                    status = (String) value;
                } else if (value instanceof String[]) {
                    final String[] array = (String[]) value;
                    if (array.length > 0) {
                        status = array[0];
                    }
                }

                if (status.trim().equalsIgnoreCase(WebConstants.ACK_BATCH_OK)) {
                    batches.add(new BatchInfo(getBatchIdFrom(batchId)));
                } else {
                    try {
                        final int lineNumber = Integer.parseInt(status.trim());
                        batches.add(new BatchInfo(getBatchIdFrom(batchId), lineNumber));
                    } catch (final NumberFormatException ex) {
                        batches.add(new BatchInfo(getBatchIdFrom(batchId),
                                BatchInfo.UNDEFINED_ERROR_LINE_NUMBER));
                    }
                }
            }
        }

        final ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());
        final IAcknowledgeService service = (IAcknowledgeService) ctx.getBean(Constants.ACKNOWLEDGE_SERVICE);
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        service.ack(batches);
    }

    private String getBatchIdFrom(final String webParameter) {
        final int index = WebConstants.ACK_BATCH_NAME.length();
        if (index >= 0) {
            return webParameter.substring(index);
        } else {
            throw new IllegalStateException("Invalid batch parameter " + webParameter);
        }
    }

    private static class BatchIdComparator implements Comparator<BatchInfo> {
        public int compare(final BatchInfo batchInfo1, final BatchInfo batchInfo2) {
            final CompareToBuilder retVal = new CompareToBuilder();
            if (batchInfo1 != null && StringUtils.isNotBlank(batchInfo1.getBatchId())
                    && StringUtils.isNumeric(batchInfo1.getBatchId()) && batchInfo2 != null
                    && StringUtils.isNotBlank(batchInfo2.getBatchId())
                    && StringUtils.isNumeric(batchInfo2.getBatchId())) {
                final Integer batchId1 = Integer.parseInt(batchInfo1.getBatchId());
                final Integer batchId2 = Integer.parseInt(batchInfo2.getBatchId());
                retVal.append(batchId1, batchId2);
            } else {
                retVal.append(batchInfo1.getBatchId(), batchInfo2.getBatchId());
            }
            return retVal.toComparison();
        }
    }
}
