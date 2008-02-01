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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.service.RegistrationNotOpenException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.web.WebConstants;

public class HttpIncomingTransport implements IIncomingTransport {

    private HttpURLConnection conn;

    private BufferedReader reader;

    public HttpIncomingTransport(HttpURLConnection connection) {
        this.conn = connection;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(reader);
    }

    public boolean isOpen() {
        return reader != null;
    }

    public BufferedReader open() throws IOException {
        if (WebConstants.REGISTRATION_NOT_OPEN == conn.getResponseCode()) {
            throw new RegistrationNotOpenException();
        } else if (WebConstants.CONNECTION_REJECTED == conn.getResponseCode()) {
            throw new ConnectionRejectedException();   
        }
        else {
            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(conn.getInputStream()), "UTF-8"));
            return reader;
        }
    }
}
