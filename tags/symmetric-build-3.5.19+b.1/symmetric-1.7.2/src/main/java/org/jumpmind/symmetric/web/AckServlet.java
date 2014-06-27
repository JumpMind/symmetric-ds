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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.handler.AckResourceHandler;

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
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        AckResourceHandler ackService = getTransportResourceHandler();
        if (logger.isDebugEnabled()) {
            logger.debug("Reading ack: " + req.getParameterMap());
        }
        // TODO: fix this; the servlets need to participate in the transport API
        List<BatchInfo> batches = AbstractTransportManager.readAcknowledgement(req.getParameterMap());
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        ackService.ack(batches);
    }

    private static class BatchIdComparator implements Comparator<BatchInfo> {
        public int compare(BatchInfo batchInfo1, BatchInfo batchInfo2) {
            Long batchId1 = batchInfo1.getBatchId();
            Long batchId2 = batchInfo1.getBatchId();
            return batchId1.compareTo(batchId2);
        }
    }

    @Override
    protected Log getLogger() {
        return logger;
    }
}
