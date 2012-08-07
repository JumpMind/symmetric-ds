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
 * under the License. 
 */

package org.jumpmind.symmetric.transport.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.web.WebConstants;

/**
 * Allow remote communication to nodes, in order to push data, pull data, and
 * send messages.
 */
public class HttpTransportManager extends AbstractTransportManager implements ITransportManager {

    private IParameterService parameterService;

    public HttpTransportManager() {
    }

    public HttpTransportManager(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.addExtensionSyncUrlHandler("httpBandwidthUrlSelector", new HttpBandwidthUrlSelector(
                engine.getNodeService(), engine.getBandwidthService()));
    }

    public int sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local,
            String securityToken, String registrationUrl) throws IOException {
        if (list != null && list.size() > 0) {
            String data = getAcknowledgementData(local.getNodeId(), list);
            return sendMessage("ack", remote, local, data, securityToken, registrationUrl);
        }
        return HttpURLConnection.HTTP_OK;
    }

    public void writeAcknowledgement(OutputStream out, List<IncomingBatch> list, Node local,
            String securityToken) throws IOException {
        writeMessage(out, getAcknowledgementData(local.getNodeId(), list));
    }

    protected int sendMessage(String action, Node remote, Node local, String data,
            String securityToken, String registrationUrl) throws IOException {
        return sendMessage(
                new URL(buildURL(action, remote, local, securityToken, registrationUrl)), data);
    }

    protected int sendMessage(URL url, String data) throws IOException {
        HttpURLConnection conn = openConnection(url, getBasicAuthUsername(), getBasicAuthPassword());
        conn.setRequestMethod("POST");
        conn.setAllowUserInteraction(false);
        conn.setDoOutput(true);
        conn.setConnectTimeout(getHttpTimeOutInMs());
        conn.setReadTimeout(getHttpTimeOutInMs());
        //conn.setRequestProperty("Content-Length", Integer.toString(data.length()));
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
        return parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_PUSH_STREAM_SIZE);
    }

    public boolean isOutputStreamEnabled() {
        return parameterService.is(ParameterConstants.TRANSPORT_HTTP_PUSH_STREAM_ENABLED);
    }

    public int getHttpTimeOutInMs() {
        return parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_TIMEOUT);
    }

    public boolean isUseCompression() {
        return parameterService.is(ParameterConstants.TRANSPORT_HTTP_USE_COMPRESSION_CLIENT);
    }

    public int getCompressionLevel() {
        return parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_LEVEL);
    }

    public int getCompressionStrategy() {
        return parameterService.getInt(ParameterConstants.TRANSPORT_HTTP_COMPRESSION_STRATEGY);
    }

    public String getBasicAuthUsername() {
        return parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_USERNAME);
    }

    public String getBasicAuthPassword() {
        return parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_PASSWORD);
    }

    public void writeMessage(OutputStream out, String data) throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, Constants.ENCODING), true);
        pw.println(data);
        pw.flush();
    }

    public IIncomingTransport getPullTransport(Node remote, Node local, String securityToken,
            Map<String, String> requestProperties, String registrationUrl) throws IOException {
        HttpURLConnection conn = createGetConnectionFor(new URL(buildURL("pull", remote, local,
                securityToken, registrationUrl)));
        if (requestProperties != null) {
            for (String key : requestProperties.keySet()) {
                conn.addRequestProperty(key, requestProperties.get(key));
            }
        }
        return new HttpIncomingTransport(conn, parameterService);
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local,
            String securityToken, String registrationUrl) throws IOException {
        URL url = new URL(buildURL("push", remote, local, securityToken, registrationUrl));
        return new HttpOutgoingTransport(url, getHttpTimeOutInMs(), isUseCompression(),
                getCompressionStrategy(), getCompressionLevel(), getBasicAuthUsername(),
                getBasicAuthPassword(), isOutputStreamEnabled(), getOutputStreamSize());
    }

    public IIncomingTransport getRegisterTransport(Node node, String registrationUrl)
            throws IOException {
        return new HttpIncomingTransport(createGetConnectionFor(new URL(buildRegistrationUrl(
                registrationUrl, node))), parameterService);
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
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
    protected String buildURL(String action, Node remote, Node local, String securityToken,
            String registrationUrl) throws IOException {
        return addSecurityToken((resolveURL(remote.getSyncUrl(), registrationUrl) + "/" + action),
                "&", local.getNodeId(), securityToken);
    }

    protected String addSecurityToken(String base, String connector, String nodeId,
            String securityToken) {
        StringBuilder sb = new StringBuilder(addNodeId(base, nodeId, "?"));
        sb.append(connector);
        sb.append(WebConstants.SECURITY_TOKEN);
        sb.append("=");
        sb.append(securityToken);
        return sb.toString();
    }

    protected String addNodeId(String base, String nodeId, String connector) {
        StringBuilder sb = new StringBuilder(base);
        sb.append(connector);
        sb.append(WebConstants.NODE_ID);
        sb.append("=");
        sb.append(nodeId);
        return sb.toString();
    }
}