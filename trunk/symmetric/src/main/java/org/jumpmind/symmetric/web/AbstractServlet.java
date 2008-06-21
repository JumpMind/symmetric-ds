/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>,
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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.SocketException;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

abstract public class AbstractServlet extends HttpServlet {

    protected abstract Log getLogger();

    protected OutputStream createOutputStream(HttpServletResponse resp) throws IOException {
        return resp.getOutputStream();
    }

    protected InputStream createInputStream(HttpServletRequest req) throws IOException {
        InputStream is = null;
        String contentType = req.getHeader("Content-Type");
        boolean useCompression = contentType != null && contentType.equalsIgnoreCase("gzip");

        if (getLogger().isDebugEnabled()) {
            StringBuilder b = new StringBuilder();
            BufferedReader reader = null;
            if (useCompression) {
                getLogger().debug("Received compressed stream");
                reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(req.getInputStream())));
            } else {
                reader = req.getReader();
            }

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
            if (useCompression) {
                is = new GZIPInputStream(is);
            }
        }

        return is;
    }

    protected ApplicationContext getDefaultApplicationContext() {
        return WebApplicationContextUtils.getWebApplicationContext(getServletContext());
    }

    @Override
    protected final void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handleGet(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing GET method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on GET method", e);
            }
            if (!resp.isCommitted()) {
                resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
    }

    /**
     * Override me to do real work. Remember that a GET should be idempotent and
     * safe. See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     * 
     */
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected final void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handlePost(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing POST method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on POST method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. See
     * http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handlePost(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected final void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handlePut(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing PUT method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on PUT method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. Remember that a PUT should be idempotent.
     * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handlePut(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected final void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        try {
            handleDelete(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing DELETE method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on DELETE method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. Remember that a DELETE should be idempotent.
     * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handleDelete(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handleHead(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing HEAD method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on HEAD method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. Remember that a HEAD should be idempotent
     * and safe. See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handleHead(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handleOptions(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing data ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on OPTIONS method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. Remember that a OPTIONS should be idempotent
     * and safe. See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handleOptions(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            handleTrace(req, resp);
        } catch (SocketException e) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("Socket issue while processing TRACE method ", e);
            }
        } catch (Exception e) {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("uncaught exception on TRACE method", e);
            }
            sendError(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Override me to do real work. Remember that a TRACE should be idempotent.
     * See http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html.
     * 
     * @param req
     * @param resp
     * @throws IOException
     * @throws ServletException
     * @throws Exception
     *                 everything else that could go wrong!
     */
    protected void handleTrace(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @throws IOException
     */
    protected boolean sendError(HttpServletResponse resp, int statusCode) throws IOException {
        return ServletUtils.sendError(resp, statusCode);
    }

    /**
     * Because you can't send an error when the response is already committed,
     * this helps to avoid unnecessary errors in the logs.
     * 
     * @param resp
     * @param statusCode
     * @param message
     *                a message to put in the body of the response
     * @throws IOException
     */
    protected boolean sendError(HttpServletResponse resp, int statusCode, String message) throws IOException {
        return ServletUtils.sendError(resp, statusCode, message);
    }

    /**
     * Returns the parameter with that name, trimmed to null
     * 
     * @param request
     * @param name
     * @return
     */
    protected String getParameter(HttpServletRequest request, String name) {
        return StringUtils.trimToNull(request.getParameter(name));
    }

    /**
     * Returns the parameter with that name, trimmed to null. If the trimmed
     * string is null, defaults to the defaultValue.
     * 
     * @param request
     * @param name
     * @return
     */
    protected String getParameter(HttpServletRequest request, String name, String defaultValue) {
        return StringUtils.defaultIfEmpty(StringUtils.trimToNull(request.getParameter(name)), defaultValue);
    }

    protected long getParameterAsNumber(HttpServletRequest request, String name) {
        return NumberUtils.toLong(StringUtils.trimToNull(request.getParameter(name)));
    }

}
