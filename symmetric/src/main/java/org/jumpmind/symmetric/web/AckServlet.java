/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Keith Naas <knaas@users.sourceforge.net>
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.transport.AckResourceHandler;

public class AckServlet extends
        AbstractTransportResourceServlet<AckResourceHandler> {

    private static final BatchIdComparator BATCH_ID_COMPARATOR = new BatchIdComparator();

    private static final long serialVersionUID = 1L;

    protected static final Log logger = LogFactory.getLog(AckServlet.class);


    @Override
    public boolean isContainerCompatible()
    {
        return true;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        Map parameters = req.getParameterMap();
        List<BatchInfo> batches = new ArrayList<BatchInfo>();

        // TODO: acks should be saved in order received
        for (Object batch : parameters.keySet()) {
            String batchId = ObjectUtils.toString(batch, "");
            if (batchId.startsWith(WebConstants.ACK_BATCH_NAME)) {
                String status = "";
                final Iterator iterator = IteratorUtils.getIterator(parameters
                        .get(batch));
                if (iterator.hasNext()) {
                    status = StringUtils.trimToEmpty(ObjectUtils.toString(
                            iterator.next(), null));
                }

                if (status.equalsIgnoreCase(WebConstants.ACK_BATCH_OK)) {
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
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        getTransportResourceHandler().ack(batches);
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

    @Override
    protected Log getLogger() {
        return logger;
    }
}
