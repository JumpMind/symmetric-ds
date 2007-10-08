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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.jumpmind.symmetric.transport.metered.MeteredOutputStreamOutgoingTransport;
import org.jumpmind.symmetric.util.MeteredOutputStream;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

abstract public class AbstractServlet extends HttpServlet {

    protected int getDownloadRate() {
        return (Integer) getContext().getBean(Constants.DOWNLOAD_RATE);
    }

    protected abstract Log getLogger();

    protected IOutgoingTransport createOutgoingTransport(
            HttpServletResponse resp) throws IOException {
        int downloadRate = getDownloadRate();
        if (downloadRate > 0) {
            return new MeteredOutputStreamOutgoingTransport(resp
                    .getOutputStream(), downloadRate*MeteredOutputStream.KB);
        } else {
            return new InternalOutgoingTransport(resp.getOutputStream());
        }
    }

    protected OutputStream createOutputStream(HttpServletResponse resp)
            throws IOException {
        int downloadRate = getDownloadRate();
        if (downloadRate > 0) {
            return new MeteredOutputStream(resp.getOutputStream(), downloadRate*MeteredOutputStream.KB);
        } else {
            return resp.getOutputStream();
        }
    }

    protected InputStream createInputStream(HttpServletRequest req)
            throws IOException {
        InputStream is = null;

        if (getLogger().isDebugEnabled()) {
            StringBuilder b = new StringBuilder();
            BufferedReader reader = req.getReader();
            String line = null;
            do {
                line = reader.readLine();
                if (line != null) {
                    b.append(line);
                    b.append("\n");
                }
            } while (line != null);

            getLogger().debug("Received: \n" + b);
            is = new ByteArrayInputStream(b.toString().getBytes());
        } else {
            is = req.getInputStream();
        }

        return is;
    }

    protected ApplicationContext getContext() {
        return WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());
    }

    protected IDataLoaderService getDataLoaderService() {
        return (IDataLoaderService) getContext().getBean(
                Constants.DATALOADER_SERVICE);
    }

    protected INodeService getNodeService() {
        return (INodeService) getContext().getBean(Constants.NODE_SERVICE);
    }

    protected IDataExtractorService getDataExtractorService() {
        return (IDataExtractorService) getContext().getBean(
                Constants.DATAEXTRACTOR_SERVICE);
    }

}
