/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Keith Naas <knaas@users.sourceforge.net>
 * Copyright (C) Eric Long <erilong@user.sourceforge.net>
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
import java.util.Enumeration;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.transport.AckResourceHandler;

public class AckServlet extends AbstractTransportResourceServlet<AckResourceHandler> {

    private static final BatchIdComparator BATCH_ID_COMPARATOR = new BatchIdComparator();

    private static final long serialVersionUID = 1L;

    protected static final Log logger = LogFactory.getLog(AckServlet.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {

        Enumeration enumeration = req.getParameterNames();
        List<BatchInfo> batches = new ArrayList<BatchInfo>();

        while (enumeration.hasMoreElements()) {
            String parameterName = (String) enumeration.nextElement();

            if (parameterName.startsWith(WebConstants.ACK_BATCH_NAME)) {
                String batchId = parameterName.substring(WebConstants.ACK_BATCH_NAME.length());
                BatchInfo batchInfo = new BatchInfo(batchId);
                batchInfo.setNodeId(getParameter(req, WebConstants.NODE_ID));
                batchInfo.setNetworkMillis(getParameterAsNumber(req, WebConstants.ACK_NETWORK_MILLIS));
                batchInfo.setFilterMillis(getParameterAsNumber(req, WebConstants.ACK_FILTER_MILLIS));
                batchInfo.setDatabaseMillis(getParameterAsNumber(req, WebConstants.ACK_DATABASE_MILLIS));
                batchInfo.setByteCount(getParameterAsNumber(req, WebConstants.ACK_BYTE_COUNT));
                String status = getParameter(req, parameterName, "");
                batchInfo.setOk(status.equalsIgnoreCase(WebConstants.ACK_BATCH_OK));

                if (!batchInfo.isOk()) {
                    batchInfo.setErrorLine(NumberUtils.toLong(status));
                    batchInfo.setSqlState(getParameter(req, WebConstants.ACK_SQL_STATE));
                    batchInfo.setSqlCode((int) getParameterAsNumber(req, WebConstants.ACK_SQL_CODE));
                    batchInfo.setSqlMessage(getParameter(req, WebConstants.ACK_SQL_MESSAGE));
                }
                batches.add(batchInfo);
            }
        }

        Collections.sort(batches, BATCH_ID_COMPARATOR);
        getTransportResourceHandler().ack(batches);
    }

    private static class BatchIdComparator implements Comparator<BatchInfo> {
        public int compare(BatchInfo batchInfo1, BatchInfo batchInfo2) {
            Long batchId1 = NumberUtils.toLong(batchInfo1.getBatchId());
            Long batchId2 = NumberUtils.toLong(batchInfo1.getBatchId());
            return batchId1.compareTo(batchId2);
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }
}
