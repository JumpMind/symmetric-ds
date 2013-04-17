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


package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.RegistrationRequiredException;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.web.WebConstants;

/**
 * 
 */
public class HttpOutgoingTransport implements IOutgoingWithResponseTransport {

    private URL url;

    private BufferedWriter writer;

    private BufferedReader reader;

    private HttpURLConnection connection;

    private int httpTimeout;

    private boolean useCompression;
    
    private int compressionStrategy;
    
    private int compressionLevel;
    
    private String basicAuthUsername;
    
    private String basicAuthPassword;
    
    private boolean streamOutputEnabled = false;
    
    private int streamOutputChunkSize = 30720;
    

    public HttpOutgoingTransport(URL url, int httpTimeout, boolean useCompression, int compressionStrategy, int compressionLevel,
            String basicAuthUsername, String basicAuthPassword, boolean streamOutputEnabled, int streamOutputSize) {
        this.url = url;
        this.httpTimeout = httpTimeout;
        this.useCompression = useCompression;
        this.compressionLevel = compressionLevel;
        this.compressionStrategy = compressionStrategy;
        this.basicAuthUsername = basicAuthUsername;
        this.basicAuthPassword = basicAuthPassword;
        this.streamOutputChunkSize = streamOutputSize;
        this.streamOutputEnabled = streamOutputEnabled;
    }

    public void close() throws IOException {
        closeWriter(true);
        closeReader();
        if (connection != null) {
            connection.disconnect();
            connection = null;
        }
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            IOUtils.closeQuietly(reader);
            reader = null;
        }
    }

    private void closeWriter(boolean closeQuietly) throws IOException {
        if (writer != null) {
            writer.flush();
            if (closeQuietly) {
                IOUtils.closeQuietly(writer);
            } else {
                writer.close();
            }
            writer = null;
        }
    }

    /**
     * Before streaming data to the remote node, make sure it is ok to. We have
     * found that we can be more efficient on a push by relying on HTTP
     * keep-alive.
     * 
     * @throws IOException
     * @throws {@link ConnectionRejectedException}
     * @throws {@link AuthenticationException}
     */
    private HttpURLConnection requestReservation() throws IOException {
        connection = HttpTransportManager.openConnection(url, basicAuthUsername, basicAuthPassword);
        connection.setUseCaches(false);
        connection.setConnectTimeout(httpTimeout);
        connection.setReadTimeout(httpTimeout);
        connection.setRequestMethod("HEAD");
        
        analyzeResponseCode(connection.getResponseCode());
        return connection;
    }

    public BufferedWriter open() throws IOException {

        connection = HttpTransportManager.openConnection(url, basicAuthUsername, basicAuthPassword);
        if (streamOutputEnabled) {
            connection.setChunkedStreamingMode(streamOutputChunkSize);
        }
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setUseCaches(false);
        connection.setConnectTimeout(httpTimeout);
        connection.setReadTimeout(httpTimeout);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("accept-encoding", "gzip");
        if (useCompression) {
            connection.addRequestProperty("Content-Type", "gzip"); // application/x-gzip?
        }

        OutputStream out = connection.getOutputStream();
        if (useCompression) {
            out = new GZIPOutputStream(out) {
                {
                    this.def.setLevel(compressionLevel);
                    this.def.setStrategy(compressionStrategy);
                }
            };
        }
        OutputStreamWriter wout = new OutputStreamWriter(out, Constants.ENCODING);
        writer = new BufferedWriter(wout);
        return writer;
    }

    /**
     * @throws {@link ConnectionRejectedException}
     * @throws {@link AuthenticationException}
     */
    private void analyzeResponseCode(int code) throws IOException {
        if (WebConstants.SC_SERVICE_UNAVAILABLE == code) {
            throw new ConnectionRejectedException();
        } else if (WebConstants.SC_FORBIDDEN == code) {
            throw new AuthenticationException();
        } else if (WebConstants.SYNC_DISABLED == code) {
            throw new SyncDisabledException();
        } else if (WebConstants.REGISTRATION_REQUIRED == code) {
            throw new RegistrationRequiredException();
        }
    }

    public BufferedReader readResponse() throws IOException {
        closeWriter(false);
        analyzeResponseCode(connection.getResponseCode());
        this.reader = HttpTransportManager.getReaderFrom(connection);
        return this.reader;
    }

    public boolean isOpen() {
        return connection != null;
    }

    public ChannelMap getSuspendIgnoreChannelLists(IConfigurationService configurationService) throws IOException {

        HttpURLConnection connection = requestReservation();

        // Connection contains remote suspend/ignore channels list if
        // reservation was successful.

        ChannelMap suspendIgnoreChannelsList = new ChannelMap();

        String suspends = connection.getHeaderField(WebConstants.SUSPENDED_CHANNELS);
        String ignores = connection.getHeaderField(WebConstants.IGNORED_CHANNELS);

        suspendIgnoreChannelsList.addSuspendChannels(suspends);
        suspendIgnoreChannelsList.addIgnoreChannels(ignores);

        return suspendIgnoreChannelsList;
    }

}