/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.web.compression;

import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.Deflater;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

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

    static final ILog log = LogFactory.getLog(CompressionFilter.class);

    int compressionLevel = Deflater.DEFAULT_COMPRESSION;

    int compressionStrategy = Deflater.DEFAULT_STRATEGY;

    /**
     * The filter configuration object we are associated with. If this value is
     * null, this filter instance is not currently configured.
     */
    private FilterConfig config = null;

    /**
     * Place this filter into service.
     * 
     * @param filterConfig
     *            The filter configuration object
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
     * The <code>doFilter</code> method of the Filter is called by the container
     * each time a request/response pair is passed through the chain due to a
     * client request for a resource at the end of the chain. The FilterChain
     * passed into this method allows the Filter to pass on the request and
     * response to the next entity in the chain.
     * <p>
     * This method first examines the request to check whether the client
     * support compression. <br>
     * It simply just pass the request and response if there is no support for
     * compression.<br>
     * If the compression support is available, it creates a
     * CompressionServletResponseWrapper object which compresses the content and
     * modifies the header if the content length is big enough. It then invokes
     * the next entity in the chain using the FilterChain object (
     * <code>chain.doFilter()</code>), <br>
     */
    @SuppressWarnings("rawtypes")
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
            ServletException {

        log.debug("CompressionFilterStarting");

        boolean supportCompression = false;
        if (request instanceof HttpServletRequest) {
            log.debug("CompressionFilterURI", ((HttpServletRequest) request).getRequestURI());

            // Are we allowed to compress ?
            String s = (String) ((HttpServletRequest) request).getParameter("gzip");
            if ("false".equals(s)) {
                log.debug("CompressionFilterNotCompressing");
                chain.doFilter(request, response);
                return;
            }

            Enumeration e = ((HttpServletRequest) request).getHeaders("Accept-Encoding");
            while (e.hasMoreElements()) {
                String name = (String) e.nextElement();
                if (name.indexOf("gzip") != -1) {
                    log.debug("CompressionFilterSupportsCompression");
                    supportCompression = true;
                } else {
                    log.debug("CompressionFilterDoesNotSupportsCompression");
                }
            }
        }

        if (!supportCompression) {
            log.debug("CompressionFilterCalledNotCompressing");
            chain.doFilter(request, response);
            return;
        } else {
            if (response instanceof HttpServletResponse) {
                CompressionServletResponseWrapper wrappedResponse = new CompressionServletResponseWrapper(
                        (HttpServletResponse) response, compressionLevel, compressionStrategy);
                log.debug("CompressionFilterCalledCompressing");
                try {
                    chain.doFilter(request, wrappedResponse);
                } finally {
                    wrappedResponse.finishResponse();
                }
                return;
            }
        }
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public int getCompressionStrategy() {
        return compressionStrategy;
    }

    public void setCompressionStrategy(int compressionStrategy) {
        this.compressionStrategy = compressionStrategy;
    }

    /**
     * Set filter config This function is equivalent to init. Required by
     * Weblogic 6.1
     * 
     * @param filterConfig
     *            The filter configuration object
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