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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.jumpmind.symmetric.web.WebConstants;

/**
 * Allow remote communication to nodes, in order to push data, pull data, and
 * send messages.
 */
public class HttpTransportManager extends AbstractTransportManager implements ITransportManager {

    protected static final Log logger = LogFactory.getLog(HttpTransportManager.class);

    int httpTimeOutInMs;
    boolean useCompression;
    int compressionLevel;
    int compressionStrategy;
    
    public HttpTransportManager(String registrationUrl) {
      this(120000, true, -1, 0, registrationUrl);   
    }
    public HttpTransportManager(int httpTimeOutInMs, boolean useCompression, int compressionLevel, int compressionStrategy, String registrationUrl) {
        super(registrationUrl);
        this.httpTimeOutInMs = httpTimeOutInMs;
        this.useCompression = useCompression;
        this.compressionLevel = compressionLevel;
        this.compressionStrategy = compressionStrategy;
    }
    
    public boolean sendAcknowledgement(Node remote, List<IncomingBatch> list, Node local, String securityToken) throws IOException {
        if (list != null && list.size() > 0) {
            String data = getAcknowledgementData(local.getNodeId(), list);
            return sendMessage("ack", remote, local, data, securityToken);
        }
        return true;
    }

    public void writeAcknowledgement(OutputStream out, List<IncomingBatch> list, Node local, String securityToken) throws IOException {
        writeMessage(out, getAcknowledgementData(local.getNodeId(), list));
    }

    protected boolean sendMessage(String action, Node remote, Node local, String data, String securityToken) throws IOException {
        HttpURLConnection conn = sendMessage(new URL(buildURL(action, remote, local, securityToken)), data);
        return conn.getResponseCode() == HttpURLConnection.HTTP_OK;
    }

    protected HttpURLConnection sendMessage(URL url, String data) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setAllowUserInteraction(false);
        conn.setDoOutput(true);
        conn.setConnectTimeout(httpTimeOutInMs);
        conn.setReadTimeout(httpTimeOutInMs);
        conn.setRequestProperty("Content-Length", Integer.toString(data.length()));
        writeMessage(conn.getOutputStream(), data);
        return conn;
    }

    public void writeMessage(OutputStream out, String data) throws IOException {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, Constants.ENCODING), true);
        pw.println(data);
        pw.close();
    }

    public IIncomingTransport getPullTransport(Node remote, Node local,
            String securityToken, Map<String, String> requestProperties)
            throws IOException {
        HttpURLConnection conn = createGetConnectionFor(new URL(buildURL(
                "pull", remote, local, securityToken)));
        if (requestProperties != null) {
            for (String key : requestProperties.keySet()) {
                conn.addRequestProperty(key, requestProperties.get(key));
            }
        }
        return new HttpIncomingTransport(conn);
    }

    public IOutgoingWithResponseTransport getPushTransport(Node remote, Node local, String securityToken) throws IOException {
        URL url = new URL(buildURL("push", remote, local, securityToken));
        return new HttpOutgoingTransport(url, httpTimeOutInMs, useCompression, compressionStrategy, compressionLevel);
    }

    public IIncomingTransport getRegisterTransport(Node node) throws IOException {
        return new HttpIncomingTransport(createGetConnectionFor(new URL(buildRegistrationUrl(registrationUrl, node))));
    }

    public static String buildRegistrationUrl(String baseUrl, Node node) throws IOException {
        StringBuilder builder = new StringBuilder(baseUrl);
        builder.append("/registration?");
        append(builder, WebConstants.NODE_GROUP_ID, node.getNodeGroupId());
        append(builder, WebConstants.EXTERNAL_ID, node.getExternalId());
        append(builder, WebConstants.SYNC_URL, node.getSyncUrl());
        append(builder, WebConstants.SCHEMA_VERSION, node.getSchemaVersion());
        append(builder, WebConstants.DATABASE_TYPE, node.getDatabaseType());
        append(builder, WebConstants.DATABASE_VERSION, node.getDatabaseVersion());
        append(builder, WebConstants.SYMMETRIC_VERSION, node.getSymmetricVersion());
        return builder.toString();
    }

    protected HttpURLConnection createGetConnectionFor(URL url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("accept-encoding", "gzip");
        conn.setConnectTimeout(httpTimeOutInMs);
        conn.setReadTimeout(httpTimeOutInMs);
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
    protected String buildURL(String action, Node remote, Node local, String securityToken) throws IOException {
        return addSecurityToken((resolveURL(remote.getSyncUrl()) + "/" + action), "&", local.getNodeId(), securityToken);
    }

    protected String addSecurityToken(String base, String connector, String nodeId, String securityToken) {
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
