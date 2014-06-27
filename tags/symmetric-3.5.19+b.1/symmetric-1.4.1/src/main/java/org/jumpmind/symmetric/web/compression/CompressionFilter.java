/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jumpmind.symmetric.web.compression;

import java.io.IOException;
import java.util.Enumeration;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of <code>javax.servlet.Filter</code> used to compress the
 * ServletResponse if it is bigger than a threshold.
 * 
 * This package is derived from the Jakarta <a
 * href="http://jakarta.apache.org/tomcat">Tomcat</a> examples compression
 * filter and is distributed in SymmetricDS for convenience.
 * 
 * @author Amy Roh
 * @author Dmitri Valdin
 * @version $Revision: 466607 $, $Date: 2006-10-21 17:09:50 -0600 (Sat, 21 Oct
 *          2006) $
 */

public class CompressionFilter implements Filter {

    static final Log logger = LogFactory.getLog(CompressionFilter.class);

    /**
     * The filter configuration object we are associated with. If this value is
     * null, this filter instance is not currently configured.
     */
    private FilterConfig config = null;

    /**
     * Place this filter into service.
     * 
     * @param filterConfig
     *                The filter configuration object
     */

    public void init(FilterConfig filterConfig) {

        config = filterConfig;
    }

    /**
     * Take this filter out of service.
     */
    public void destroy() {

        this.config = null;

    }

    /**
     * The <code>doFilter</code> method of the Filter is called by the
     * container each time a request/response pair is passed through the chain
     * due to a client request for a resource at the end of the chain. The
     * FilterChain passed into this method allows the Filter to pass on the
     * request and response to the next entity in the chain.
     * <p>
     * This method first examines the request to check whether the client
     * support compression. <br>
     * It simply just pass the request and response if there is no support for
     * compression.<br>
     * If the compression support is available, it creates a
     * CompressionServletResponseWrapper object which compresses the content and
     * modifies the header if the content length is big enough. It then invokes
     * the next entity in the chain using the FilterChain object (<code>chain.doFilter()</code>),
     * <br>
     */

    @SuppressWarnings("unchecked")
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {

        if (logger.isDebugEnabled()) {
            logger.debug("@doFilter");
        }

        boolean supportCompression = false;
        if (request instanceof HttpServletRequest) {
            if (logger.isDebugEnabled()) {
                logger.debug("requestURI = " + ((HttpServletRequest) request).getRequestURI());
            }

            // Are we allowed to compress ?
            String s = (String) ((HttpServletRequest) request).getParameter("gzip");
            if ("false".equals(s)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("got parameter gzip=false --> don't compress, just chain filter");
                }
                chain.doFilter(request, response);
                return;
            }

            Enumeration e = ((HttpServletRequest) request).getHeaders("Accept-Encoding");
            while (e.hasMoreElements()) {
                String name = (String) e.nextElement();
                if (name.indexOf("gzip") != -1) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("supports compression");
                    }
                    supportCompression = true;
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("no support for compresion");
                    }
                }
            }
        }

        if (!supportCompression) {
            if (logger.isDebugEnabled()) {
                logger.debug("doFilter gets called wo compression");
            }
            chain.doFilter(request, response);
            return;
        } else {
            if (response instanceof HttpServletResponse) {
                CompressionServletResponseWrapper wrappedResponse = new CompressionServletResponseWrapper(
                        (HttpServletResponse) response);
                if (logger.isDebugEnabled()) {
                    logger.debug("doFilter gets called with compression");
                }
                try {
                    chain.doFilter(request, wrappedResponse);
                } finally {
                    wrappedResponse.finishResponse();
                }
                return;
            }
        }
    }

    /**
     * Set filter config This function is equivalent to init. Required by
     * Weblogic 6.1
     * 
     * @param filterConfig
     *                The filter configuration object
     */
    public void setFilterConfig(FilterConfig filterConfig) {
        init(filterConfig);
    }

    /**
     * Return filter config Required by Weblogic 6.1
     */
    public FilterConfig getFilterConfig() {
        return config;
    }

}
