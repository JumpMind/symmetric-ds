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

import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class HttpServletOutgoingTransport implements IOutgoingTransport {
    private HttpServletResponse response;

    public HttpServletOutgoingTransport(HttpServletResponse resp) {
        this.response = resp;
    }

    public void close() throws IOException {
    }

    public BufferedWriter open() throws IOException {
        return new BufferedWriter(response.getWriter());
    }

    public BufferedReader readResponse() throws IOException {
        throw new UnsupportedOperationException();
    }

    public boolean isOpen() {
        return response != null;
    }

}
