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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;

public class HttpOutgoingTransport implements IOutgoingWithResponseTransport {

    private URL url;

    private BufferedWriter writer;

    private BufferedReader reader;

    private HttpURLConnection connection;

    private int httpTimeout;

    private boolean useCompression;

    public HttpOutgoingTransport(URL url, int httpTimeout, boolean useCompression) {
        this.url = url;
        this.httpTimeout = httpTimeout;
        this.useCompression = useCompression;
    }

    public void close() throws IOException {
        closeWriter();
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

    private void closeWriter() throws IOException {
        if (writer != null) {
            writer.flush();
            IOUtils.closeQuietly(writer);
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
    private void requestReservation() throws IOException {
        HttpURLConnection c = (HttpURLConnection) url.openConnection();
        c.setUseCaches(false);
        c.setConnectTimeout(httpTimeout);
        c.setReadTimeout(httpTimeout);
        c.setRequestMethod("HEAD");
        analyzeResponseCode(c.getResponseCode());
    }

    public BufferedWriter open() throws IOException {
        requestReservation();
        connection = (HttpURLConnection) url.openConnection();
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
            out = new GZIPOutputStream(out);
        }
        OutputStreamWriter wout = new OutputStreamWriter(out, "UTF-8");
        writer = new BufferedWriter(wout);
        return writer;
    }

    /**
     * @throws {@link ConnectionRejectedException}
     * @throws {@link AuthenticationException}
     */
    private void analyzeResponseCode(int code) throws IOException {
        if (HttpServletResponse.SC_SERVICE_UNAVAILABLE == code) {
            throw new ConnectionRejectedException();
        } else if (HttpServletResponse.SC_FORBIDDEN == code) {
            throw new AuthenticationException();
        }
    }

    public BufferedReader readResponse() throws IOException {
        closeWriter();
        analyzeResponseCode(connection.getResponseCode());
        this.reader = HttpTransportManager.getReaderFrom(connection);
        return this.reader;
    }

    public boolean isOpen() {
        return connection != null;
    }

}
