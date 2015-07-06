/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.transport.http;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.jumpmind.symmetric.web.WebConstants.MAKE_RESERVATION_PATH;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.IoConstants;
import org.jumpmind.symmetric.model.BatchId;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.symmetric.web.WebConstants;
import org.jumpmind.util.AppUtils;

/**
 * Allow remote communication to nodes, in order to push data, pull data, and
 * send messages.
 */
public class HttpTransportManager extends AbstractTransportManager implements ITransportManager {

    private ISymmetricEngine engine;

    public HttpTransportManager() {
    }

    public HttpTransportManager(ISymmetricEngine engine) {
        super(engine.getExtensionService());
        this.engine = engine;
    }

    public int sendCopyRequest(Node local) throws IOException {
        StringBuilder data = new StringBuilder();
        Map<String, BatchId> batchIds = engine.getIncomingBatchService().findMaxBatchIdsByChannel();
        for (String channelId : batchIds.keySet()) {
            if (!Constants.CHANNEL_CONFIG.equals(channelId) && !Constants.CHANNEL_HEARTBEAT.equals(channelId)) {
                BatchId batchId = batchIds.get(channelId);
                append(data, channelId + "-" + batchId.getNodeId(), batchId.getBatchId());
            }
        }
        String securityToken = engine.getNodeService().findNodeSecurity(local.getNodeId())
                .getNodePassword();
        String url = addParameterTokens(engine.getParameterService().getRegistrationUrl() + "/copy",
                local.getNodeId(), null, securityToken);
        url = add(url, WebConstants.EXTERNAL_ID, engine.getParameterService().getExternalId(), "&");
        url = add(url, WebConstants.NODE_GROUP_ID, engine.getParameterService().getNodeGroupId(), "&");

        log.info("Contact server to do node copy using a url of: " + url);
        return sendMessage(new URL(url), data.toString());
    }

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local,
            String securityToken, String registrationUrl) throws IOException {
        if (list != null && list.size() > 0) {
            String data = getAcknowledgementData(remote.requires13Compatiblity(), local.getNodeId(), list);
            return sendMessage("ack", remote, local, data, securityToken, registrationUrl);
        }
        return HttpURLConnection.HTTP_OK;
    }

    public void writeAcknowledgement(OutputStream out, Node remote, List<IncomingBatch> list, Node local,
            String securityToken) throws IOException {
        writeMessage(out, getAcknowledgementData(remote.requires13Compatiblity(), local.getNodeId(), list));
    }

    protected int sendMessage(String action, Node remote, Node local, String data,
            String securityToken, String registrationUrl) throws IOException {
        return sendMessage(
                new URL(buildURL(action, remote, local, securityToken, null, registrationUrl)), data);
    }

    protected int sendMessage(URL url, String data) throws IOException {
        HttpURLConnection conn = openConnection(url, getBasicAuthUsername(), getBasicAuthPassword());
        conn.setRequestMethod("POST");
        conn.setAllowUserInteraction(false);
        conn.setDoOutput(true);
        conn.setConnectTimeout(getHttpTimeOutInMs());
        conn.setReadTimeout(getHttpTimeOutInMs());
        OutputStream os = conn.getOutputStream();
        try {
            writeMessage(os, data);
            return conn.getResponseCode();
        } finally {
            IOUtils.closeQuietly(os);
        }
    }

    public static HttpURLConnection openConnection(URL url, String username, String password)
            throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();        
        conn.setRequestProperty(WebConstants.HEADER_ACCEPT_CHARSET, IoConstants.ENCODING);
        setBasicAuthIfNeeded(conn, username, password);
        return conn;
    }

    public static void setBasicAuthIfNeeded(HttpURLConnection conn, String username, String password) {
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            String userpassword = username + ":" + password;
            String encodedAuthorization = new String(Base64.encodeBase64(userpassword.getBytes()));
            conn.setRequestProperty("Authorization", "Basic " + encodedAuthorization);
        }
    }

    public int getOutputStreamSize() {
        return engine.getParameterService().getInt(ParameterConstants.TRANSPORT_HTTP_PUSH_STREAM_SIZE);
    }

    public boolean isOutputStreamEnabled() {
        return engine.getParameterService().is(ParameterConstants.TRANSPORT_HTTP_PUSH_STREAM_ENABLED);
    }

    public int getHttpTimeOutInMs() {
        return engine.getParameterService().getInt(ParameterConstants.TRANSPORT_HTTP_TIMEOUT);
    }

    public boolean isUseCompression() {
        return engine.getParameterService().is(ParameterConstants.TRANSPORT_HTTP_USE_COMPRESSION_CLIENT);
    }

    public int getCompressionLevel() {
        return engine.getParameterService().getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_LEVEL);
    }

    public int getCompressionStrategy() {
        return engine.getParameterService().getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_STRATEGY);
    }

    public String getBasicAuthUsername() {
        return engine.getParameterService().getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_USERNAME);
    }

    public String getBasicAuthPassword() {
        return engine.getParameterService().getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_PASSWORD);
    }

    public void writeMessage(OutputStream out, String data) throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, IoConstants.ENCODING), true);
        pw.println(data);
        pw.flush();
    }
    
    public IIncomingTransport getFilePullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException {
        HttpURLConnection conn = createGetConnectionFor(new URL(buildURL("filesync/pull", remote, local,
                securityToken, null, registrationUrl)));
        if (requestProperties != null) {
            for (String key : requestProperties.keySet()) {
                conn.addRequestProperty(key, requestProperties.get(key));
            }
        }
        return new HttpIncomingTransport(conn, engine.getParameterService());
    }

    public IIncomingTransport getPullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException {
        HttpURLConnection conn = createGetConnectionFor(new URL(buildURL("pull", remote, local,
                securityToken, null, registrationUrl)));
        if (requestProperties != null) {
            for (String key : requestProperties.keySet()) {
                conn.addRequestProperty(key, requestProperties.get(key));
            }
        }
        return new HttpIncomingTransport(conn, engine.getParameterService());
    }
    
    public IIncomingTransport getAckStatusTransport(OutgoingBatch batch, Node remote, Node local, String securityToken, String registrationUrl) {
        try {
            HttpURLConnection conn = createGetConnectionFor(new URL(buildURL(String.format("ackstatus/%s", batch.getBatchId()), remote,
                    local, securityToken, null, registrationUrl)));
            return new HttpIncomingTransport(conn, engine.getParameterService());
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }
    
    @Override
    public void makeReservationTransport(String poolId, String channelId, Node remote, Node local, String securityToken,
            String registrationUrl) {
        try {
            HttpURLConnection conn = createGetConnectionFor(new URL(buildURL(String.format("%s/%s", MAKE_RESERVATION_PATH, poolId), remote,
                    local, securityToken, channelId, registrationUrl)));
            conn.connect();
            SymmetricUtils.analyzeResponseCode(conn.getResponseCode());
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }   

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local,
            String securityToken, String channelId, String registrationUrl) throws IOException {
        URL url = new URL(buildURL("push", remote, local, securityToken, channelId, registrationUrl));
        return new HttpOutgoingTransport(url, getHttpTimeOutInMs(), isUseCompression(),
                getCompressionStrategy(), getCompressionLevel(), getBasicAuthUsername(),
                getBasicAuthPassword(), isOutputStreamEnabled(), getOutputStreamSize(), false);
    }
    
    public IOutgoingWithResponseTransport getFilePushTransport(Node remote, Node local,
            String securityToken, String registrationUrl) throws IOException {
        URL url = new URL(buildURL("filesync/push", remote, local, securityToken, null, registrationUrl));
        return new HttpOutgoingTransport(url, getHttpTimeOutInMs(), isUseCompression(),
                getCompressionStrategy(), getCompressionLevel(), getBasicAuthUsername(),
                getBasicAuthPassword(), isOutputStreamEnabled(), getOutputStreamSize(), true);
    }    

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl)
            throws IOException {
        return new HttpIncomingTransport(createGetConnectionFor(new URL(buildRegistrationUrl(
                registrationUrl, node))), engine.getParameterService());
    }

    public static String buildRegistrationUrl(String baseUrl, Node node) throws IOException {
        if (baseUrl == null) {
            baseUrl = "";
        }
        StringBuilder builder = new StringBuilder(baseUrl);
        builder.append("/registration?");
        append(builder, WebConstants.NODE_GROUP_ID, node.getNodeGroupId());
        append(builder, WebConstants.EXTERNAL_ID, node.getExternalId());
        append(builder, WebConstants.SYNC_URL, node.getSyncUrl());
        append(builder, WebConstants.SCHEMA_VERSION, node.getSchemaVersion());
        append(builder, WebConstants.DATABASE_TYPE, node.getDatabaseType());
        append(builder, WebConstants.DATABASE_VERSION, node.getDatabaseVersion());
        append(builder, WebConstants.SYMMETRIC_VERSION, node.getSymmetricVersion());
        append(builder, WebConstants.HOST_NAME, AppUtils.getHostName());
        append(builder, WebConstants.IP_ADDRESS, AppUtils.getIpAddress());
        return builder.toString();
    }

    protected HttpURLConnection createGetConnectionFor(URL url) throws IOException {
        HttpURLConnection conn = HttpTransportManager.openConnection(url, getBasicAuthUsername(),
                getBasicAuthPassword());
        conn.setRequestProperty("accept-encoding", "gzip");
        conn.setConnectTimeout(getHttpTimeOutInMs());
        conn.setReadTimeout(getHttpTimeOutInMs());
        conn.setRequestMethod("GET");
        return conn;
    }
    
    protected static InputStream getInputStreamFrom(HttpURLConnection connection) throws IOException {
        String type = connection.getContentEncoding();
        InputStream in = connection.getInputStream();
        if (!StringUtils.isBlank(type) && type.equals("gzip")) {
            in = new GZIPInputStream(in);
        }
        return in;
    }

    /**
     * If the content is gzip'd, then uncompress.
     */
    protected static BufferedReader getReaderFrom(HttpURLConnection connection) throws IOException {
        String type = connection.getContentEncoding();
        InputStream in = connection.getInputStream();
        if (!StringUtils.isBlank(type) && type.equals("gzip")) {
            in = new GZIPInputStream(in);
        }
        return TransportUtils.toReader(in);
    }

    /**
     * Build a url for an action. Include the nodeid and the security token.
     */
    protected String buildURL(String action, Node remote, Node local, String securityToken, String channelId,
            String registrationUrl) throws IOException {
        return addParameterTokens((resolveURL(remote.getSyncUrl(), registrationUrl) + "/" + action),
                 local.getNodeId(), channelId, securityToken);
    }

    protected String addParameterTokens(String base, String nodeId, String channelId,
            String securityToken) {
        StringBuilder sb = new StringBuilder(addNodeId(base, nodeId, "?"));
        sb.append("&");
        sb.append(WebConstants.SECURITY_TOKEN);
        sb.append("=");
        sb.append(securityToken);
        if (isNotBlank(channelId)) {
            append(sb, WebConstants.CHANNEL_ID, channelId);
        }
        append(sb, WebConstants.HOST_NAME, AppUtils.getHostName());
        append(sb, WebConstants.IP_ADDRESS, AppUtils.getIpAddress());        
        return sb.toString();
    }

    protected String addNodeId(String base, String nodeId, String connector) {
        return add(base, WebConstants.NODE_ID, nodeId, connector);
    }
    
    protected String add(String base, String key, String value, String connector) {
        StringBuilder sb = new StringBuilder(base);
        sb.append(connector);
        sb.append(key);
        sb.append("=");
        try {
            sb.append(URLEncoder.encode(value, IoConstants.ENCODING));
        } catch (UnsupportedEncodingException e) {
            throw new IoException(e);
        }
        return sb.toString();
    }
}