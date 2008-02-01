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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpOutgoingTransport implements IOutgoingWithResponseTransport {

    URL url;

    BufferedWriter writer;

    BufferedReader reader;

    HttpURLConnection connection;
    
    int httpTimeout;

    public HttpOutgoingTransport(URL url, int httpTimeout) {
        this.url = url;
        this.httpTimeout = httpTimeout;
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

    public BufferedWriter open() throws IOException {
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);
        connection.setDoOutput(true);
        connection.setUseCaches(false);
        connection.setConnectTimeout(httpTimeout);
        connection.setReadTimeout(httpTimeout);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("accept-encoding", "gzip");
        OutputStream out = connection.getOutputStream();
        OutputStreamWriter wout = new OutputStreamWriter(out, "UTF-8");
        writer = new BufferedWriter(wout);
        return writer;
    }

    public BufferedReader readResponse() throws IOException {
        closeWriter();
        if (WebConstants.CONNECTION_REJECTED == connection.getResponseCode()) {
            throw new ConnectionRejectedException();   
        } else if (HttpServletResponse.SC_FORBIDDEN == connection.getResponseCode()) {
            throw new AuthenticationException();
        }
        InputStream in = new GZIPInputStream(connection.getInputStream());
        reader = new BufferedReader(new InputStreamReader(in));
        return reader;
    }

    public boolean isOpen() {
        return connection != null;
    }

}
