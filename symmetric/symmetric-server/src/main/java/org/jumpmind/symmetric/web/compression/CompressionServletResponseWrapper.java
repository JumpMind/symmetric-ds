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
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.zip.Deflater;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;

/**
 * Implementation of <b>HttpServletResponseWrapper</b> that works with the
 * CompressionServletResponseStream implementation..
 * 
 * This package is derived from the Jakarta <a
 * href="http://jakarta.apache.org/tomcat">Tomcat</a> examples compression
 * filter and is distributed in SymmetricDS for convenience.
 * 
 * @author Amy Roh
 * @author Dmitri Valdin
 * @version $Revision: 496190 $, $Date: 2007-01-14 16:21:45 -0700 (Sun, 14 Jan
 *          2007) $
 */

public class CompressionServletResponseWrapper extends HttpServletResponseWrapper {

    static final Log log = LogFactory.getLog(CompressionServletResponseWrapper.class);

    int compressionLevel = Deflater.DEFAULT_COMPRESSION;

    int compressionStrategy = Deflater.DEFAULT_STRATEGY;

    /**
     * Calls the parent constructor which creates a ServletResponse adaptor
     * wrapping the given response object.
     */
    public CompressionServletResponseWrapper(HttpServletResponse response, int compressionLevel, int compressionStrategy) {
        super(response);
        this.compressionLevel = compressionLevel;
        this.compressionStrategy = compressionStrategy;
        origResponse = response;
        log.debug("CompressionServletStarting");
    }

    /**
     * Original response
     */

    protected HttpServletResponse origResponse = null;

    /**
     * Descriptive information about this Response implementation.
     */

    protected static final String info = "CompressionServletResponseWrapper";

    /**
     * The ServletOutputStream that has been returned by
     * <code>getOutputStream()</code>, if any.
     */

    protected ServletOutputStream stream = null;

    /**
     * The PrintWriter that has been returned by <code>getWriter()</code>, if
     * any.
     */

    protected PrintWriter writer = null;

    /**
     * Content type
     */
    protected String contentType = null;

    // --------------------------------------------------------- Public Methods

    /**
     * Set content type
     */
    public void setContentType(String contentType) {
        log.debug("CompressionServletSettingType", contentType);
        this.contentType = contentType;
        origResponse.setContentType(contentType);
    }

    /**
     * Create and return a ServletOutputStream to write the content associated
     * with this Response.
     * 
     * @exception IOException
     *                if an input/output error occurs
     */
    public ServletOutputStream createOutputStream() throws IOException {
        log.debug("CompressionServletCreatingStream");
        CompressionResponseStream stream = new CompressionResponseStream(origResponse, compressionLevel,
                compressionStrategy);
        return stream;

    }

    /**
     * Finish a response.
     */
    public void finishResponse() {
        try {
            if (writer != null) {
                writer.close();
            } else {
                if (stream != null)
                    stream.close();
            }
        } catch (IOException e) {
        }
    }

    // ------------------------------------------------ ServletResponse Methods

    /**
     * Flush the buffer and commit this response.
     * 
     * @exception IOException
     *                if an input/output error occurs
     */
    public void flushBuffer() throws IOException {
        log.debug("CompressionServletFlushingBuffer");
        ((CompressionResponseStream) stream).flush();
    }

    /**
     * Return the servlet output stream associated with this Response.
     * 
     * @exception IllegalStateException
     *                if <code>getWriter</code> has already been called for this
     *                response
     * @exception IOException
     *                if an input/output error occurs
     */
    public ServletOutputStream getOutputStream() throws IOException {

        if (writer != null)
            throw new IllegalStateException("getWriter() has already been called for this response");

        if (stream == null)
            stream = createOutputStream();
        log.debug("CompressionServletStreamSettingOutput", stream);
        return (stream);
    }

    /**
     * Return the writer associated with this Response.
     * 
     * @exception IllegalStateException
     *                if <code>getOutputStream</code> has already been called
     *                for this response
     * @exception IOException
     *                if an input/output error occurs
     */
    public PrintWriter getWriter() throws IOException {

        if (writer != null)
            return (writer);

        if (stream != null)
            throw new IllegalStateException("getOutputStream() has already been called for this response");

        stream = createOutputStream();
        log.debug("CompressionServletStreamSettingWriter", stream);
        // String charset = getCharsetFromContentType(contentType);
        String charEnc = origResponse.getCharacterEncoding();
        log.debug("CompressionServletCharacterEncoding", charEnc);
        // HttpServletResponse.getCharacterEncoding() shouldn't return null
        // according the spec, so feel free to remove that "if"
        if (charEnc != null) {
            writer = new PrintWriter(new OutputStreamWriter(stream, charEnc));
        } else {
            writer = new PrintWriter(stream);
        }

        return (writer);

    }

    public void setContentLength(int length) {
    }

}