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


package org.jumpmind.symmetric.web;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;

/**
 * Super class for SymmetricDS Servlets.  Contains useful Servlet processing methods.
 */
abstract public class AbstractServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    final protected ILog log = LogFactory.getLog(getClass());

    protected OutputStream createOutputStream(HttpServletResponse resp) throws IOException {
        return resp.getOutputStream();
    }

    protected ILog getLog() {
        return this.log;
    }

    protected InputStream createInputStream(HttpServletRequest req) throws IOException {
        InputStream is = null;
        String contentType = req.getHeader("Content-Type");
        boolean useCompression = contentType != null && contentType.equalsIgnoreCase("gzip");

        if (getLog().isDebugEnabled()) {
            StringBuilder b = new StringBuilder();
            BufferedReader reader = null;
            if (useCompression) {
                getLog().debug("ServletCompressedStreamReceived");
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

            getLog().debug("ServletReceived", b);
            is = new ByteArrayInputStream(b.toString().getBytes());
        } else {
            is = req.getInputStream();
            if (useCompression) {
                is = new GZIPInputStream(is);
            }
        }

        return is;
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
     *            a message to put in the body of the response
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